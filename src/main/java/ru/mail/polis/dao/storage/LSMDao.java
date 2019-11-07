package ru.mail.polis.dao.storage;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.storage.cell.Cell;
import ru.mail.polis.dao.storage.table.FlushTable;
import ru.mail.polis.dao.storage.table.MemoryTablePool;
import ru.mail.polis.dao.storage.table.SSTable;
import ru.mail.polis.utils.GenerationUtils;
import ru.mail.polis.utils.IteratorUtils;

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
import java.util.NavigableMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class LSMDao implements DAO {

    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
    public static final String SUFFIX_DAT = ".dat";
    public static final String FILE_NAME = "SSTable_";
    public static final String SUFFIX_TMP = ".tmp";

    private static final Pattern FILE_NAME_PATTERN = Pattern.compile(FILE_NAME);
    private static final Logger logger = LoggerFactory.getLogger(LSMDao.class);

    private final File directory;
    private final MemoryTablePool memoryTablePool;
    private final Thread flushedThread;
    private final NavigableMap<Long, SSTable> ssTables;

    private long generation;

    /**
     * Creates persistence Dao based on LSMTree.
     *
     * @param flushLimit is the limit upon reaching which we write data in disk
     * @param directory  is the base directory, where contains our database
     * @throws IOException of an I/O error occurred
     */
    public LSMDao(@NotNull final File directory,
                  final long flushLimit) throws IOException {
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
        this.generation = maxGeneration.get();
        flushedThread = new Thread(new FlusherTask());
        flushedThread.start();
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        return Iterators.transform(cellIterator(from), cluster -> {
            assert cluster != null;
            return Record.of(cluster.getKey(), cluster.getValue().getData());
        });
    }

    @NotNull
    private Iterator<Cell> cellIterator(@NotNull final ByteBuffer from) {
        return IteratorUtils.data(memoryTablePool, ssTables, from);
    }

    @NotNull
    @Override
    public Iterator<Cell> latestIterator(@NotNull final ByteBuffer from) {
        return IteratorUtils.latestIter(memoryTablePool, ssTables, from);
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
        memoryTablePool.compact(ssTables, directory, generation);
    }

    private void flush(final long currentGeneration,
                       @NotNull final Iterator<Cell> data) throws IOException {
        if (data.hasNext()) {
            final File sstable = new File(directory, FILE_NAME + currentGeneration + SUFFIX_DAT);
            SSTable.writeToFile(data, sstable);
            this.generation = memoryTablePool.generation();
        }
    }

    private final class FlusherTask implements Runnable {
        @Override
        public void run() {
            boolean poisonReceived = false;
            while (!Thread.currentThread().isInterrupted() && !poisonReceived) {
                FlushTable flushTable;
                try {
                    flushTable = memoryTablePool.tableToFlush();
                    final Iterator<Cell> data = flushTable.data();
                    final long currentGeneration = flushTable.getGeneration();
                    poisonReceived = flushTable.isPoisonPills();
                    flush(currentGeneration, data);
                    memoryTablePool.flushed(currentGeneration);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    logger.info("IOError: {}", e.getMessage());
                }
            }
        }
    }
}
