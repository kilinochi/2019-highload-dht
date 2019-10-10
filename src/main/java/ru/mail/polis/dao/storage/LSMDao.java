package ru.mail.polis.dao.storage;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.storage.cluster.Cluster;
import ru.mail.polis.dao.storage.table.FlushTable;
import ru.mail.polis.dao.storage.table.MemoryTablePool;
import ru.mail.polis.dao.storage.table.SSTable;
import ru.mail.polis.dao.storage.utils.GenerationUtils;
import ru.mail.polis.dao.storage.utils.IteratorUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class LSMDao implements DAO {

    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private static final String SUFFIX_DAT = ".dat";
    private static final String FILE_NAME = "SSTable_";
    private static final Logger logger = LoggerFactory.getLogger(LSMDao.class);
    private static final Pattern FILE_NAME_PATTERN = Pattern.compile(FILE_NAME);

    private final File directory;
    private final MemoryTablePool memoryTablePool;
    private final Thread flushedThread;

    private NavigableMap<Long, SSTable> ssTables;


    /**
     * Creates persistence Dao based on LSMTree.
     *
     * @param flushLimit is the limit upon reaching which we write data in disk
     * @param directory  is the base directory, where contains our database
     * @throws IOException of an I/O error occurred
     *
     * */
    public LSMDao(@NotNull final File directory,
                  final long flushLimit) throws IOException {
        logger.info("Create dao :" + this.toString());
        this.directory = directory;
        ssTables = new ConcurrentSkipListMap<>();
        final AtomicLong maxGeneration = new AtomicLong();
        Files.walkFileTree(directory.toPath(), EnumSet.noneOf(FileVisitOption.class), 1, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path path, final BasicFileAttributes attrs)
                    throws IOException {
                final Matcher matcher = FILE_NAME_PATTERN.matcher(path.toString());
                if (path.toString().endsWith(SUFFIX_DAT) && matcher.find()) {
                    final long currentGeneration = GenerationUtils.fromPath(path);
                    maxGeneration.set(Math.max(maxGeneration.get(), currentGeneration));
                    ssTables.put(currentGeneration, new SSTable(path.toFile(), currentGeneration));
                }
                return FileVisitResult.CONTINUE;
            }
        });
        maxGeneration.set(maxGeneration.get() + 1);
        memoryTablePool = new MemoryTablePool(flushLimit, maxGeneration.get());
        flushedThread = new Thread(new FlusherTask());
        flushedThread.start();
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        return Iterators.transform(clusterIterator(from), cluster -> {
            assert cluster != null;
            return Record.of(cluster.getKey(), cluster.getClusterValue().getData());
        });
    }

    private Iterator<Cluster> clusterIterator(@NotNull final ByteBuffer from) {
        return IteratorUtils.data(memoryTablePool, ssTables, from);
    }

    private void compactDir(final long preGener) throws IOException {
        ssTables = new ConcurrentSkipListMap<>();
        Files.walkFileTree(directory.toPath(), EnumSet.noneOf(FileVisitOption.class), 1, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path path, final BasicFileAttributes attrs)
                    throws IOException {
                final File file = path.toFile();
                final Matcher matcher = FILE_NAME_PATTERN.matcher(file.getName());
                if (file.getName().endsWith(SUFFIX_DAT) && matcher.find()) {
                    final long currentGeneration = GenerationUtils.fromPath(path);
                    if(currentGeneration >= preGener) {
                        ssTables.put(currentGeneration, new SSTable(file, currentGeneration));
                        return FileVisitResult.CONTINUE;
                    }
                }
                Files.delete(path);
                return FileVisitResult.CONTINUE;
            }
        });
        logger.info("Compaction done in time: " + System.currentTimeMillis());
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memoryTablePool.upsert(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memoryTablePool.remove(key);
    }

    @Override
    public void close() throws IOException {
        memoryTablePool.close();
        try {
            flushedThread.join();
        } catch (InterruptedException e) {
           Thread.currentThread().interrupt();
        }
    }

    @Override
    public void compact() throws IOException {
        logger.info("Compaction table with size: " + ssTables.size() + " and time: " + System.currentTimeMillis());
        memoryTablePool.compact(ssTables);
    }

    private void flush(final long currentGeneration,
                       final boolean isCompactFlush,
                       @NotNull final Iterator<Cluster> data) throws IOException {
        final long startFlushTime = System.currentTimeMillis();
        logger.info("Flush start in: " + startFlushTime + " with generation: " + currentGeneration);
        if(data.hasNext()) {
           final File sstable =  new File(directory, FILE_NAME + currentGeneration + SUFFIX_DAT);
           SSTable.writeToFile(data, sstable);
           if(isCompactFlush) {
               ssTables.put(currentGeneration, new SSTable(sstable, currentGeneration));
           }
        }
        logger.info("Flush end in: " + System.currentTimeMillis() + " with generation: " + currentGeneration);
        logger.info("Estimated time: " + (System.currentTimeMillis() - startFlushTime));
    }

    private final class FlusherTask implements Runnable {

        @Override
        public void run() {
            boolean poisonReceived = false;
            while (!Thread.currentThread().isInterrupted() && !poisonReceived) {
                FlushTable flushTable;
                try {
                    logger.info("Prepare to flush in flusher task: " + this.toString());
                    flushTable = memoryTablePool.tableToFlush();
                    final Iterator<Cluster> data = flushTable.data();
                    final long currentGeneration = flushTable.getGeneration();
                    poisonReceived = flushTable.isPoisonPills();
                    final boolean isCompactTable = flushTable.isCompactionTable();
                    if(isCompactTable || poisonReceived) {
                        flush(currentGeneration, true ,data);
                    } else {
                        flush(currentGeneration, false, data);
                    }
                    if(isCompactTable) {
                        compactDir(currentGeneration);
                        memoryTablePool.switchCompaction();
                    } else {
                        memoryTablePool.flushed(currentGeneration);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    logger.info("Error :" + e.getMessage());
                }
            }
        }
    }
}