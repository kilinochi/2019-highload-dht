package ru.mail.polis.dao.storage;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.storage.cluster.Cluster;
import ru.mail.polis.dao.storage.task.FlusherTask;
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
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class LSMDao implements DAO {

    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
    private static final String SUFFIX_DAT = ".dat";
    private static final String SUFFIX_TMP = ".tmp";
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
        final FlusherTask flusherTask = new FlusherTask(memoryTablePool, this);
        flushedThread = new Thread(flusherTask);
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
        logger.info("Compaction table with size: " + ssTables.size());
        memoryTablePool.compact(ssTables);
    }

    @Override
    public void flush(long generation, final boolean isCompactFlush, @NotNull Iterator<Cluster> data) throws IOException {
        final long startFlushTime = System.currentTimeMillis();
        logger.info("Flush start in: " + startFlushTime + " with generation: " + generation);

        if(data.hasNext()) {
            final File tmp = new File(directory, FILE_NAME + generation + SUFFIX_TMP);
            SSTable.writeToFile(data, tmp);
            final File database = new File(directory, FILE_NAME + generation + SUFFIX_DAT);
            Files.move(tmp.toPath(), database.toPath(), StandardCopyOption.ATOMIC_MOVE);
        }
        if(isCompactFlush) {
            for(final SSTable ssTable : ssTables.values()) {
                Files.delete(ssTable.getTable().toPath());
            }
            ssTables = new ConcurrentSkipListMap<>();
            ssTables.put(generation, new SSTable(new File(directory, FILE_NAME + --generation + SUFFIX_DAT), --generation));
        }
        logger.info("Flush end in: " + System.currentTimeMillis() + " with generation: " + generation);
        logger.info("Estimated time: " + (System.currentTimeMillis() - startFlushTime));
    }
}