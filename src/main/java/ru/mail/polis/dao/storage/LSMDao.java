package ru.mail.polis.dao.storage;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.Iters;
import ru.mail.polis.dao.storage.cluster.Cluster;
import ru.mail.polis.dao.storage.table.MemoryTablePool;
import ru.mail.polis.dao.storage.table.SSTable;
import ru.mail.polis.dao.storage.table.TableToFlush;
import ru.mail.polis.dao.storage.utils.GenerationUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class LSMDao implements DAO {

    private static final Logger logger = LoggerFactory.getLogger(LSMDao.class);

    private static final String SUFFIX_DAT = ".dat";
    private static final String SUFFIX_TMP = ".tmp";
    private static final String FILE_NAME = "SSTable_";
    private static final Pattern FILE_NAME_PATTERN = Pattern.compile(FILE_NAME);
    private static final ByteBuffer SMALLEST_KEY = ByteBuffer.allocate(0);

    private final File directory;
    private final long compactLimit;
    private final Thread flusherThread;
    private final MemoryTablePool memoryTablePool;
    private final ScheduledExecutorService scheduledExecutorService;

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
                  final long compactLimit,
                  final long flushLimit) throws IOException {
        logger.info("Create dao :" + this.toString());
        this.compactLimit = compactLimit;
        this.directory = directory;
        ssTables = new ConcurrentSkipListMap<>();
        long maxGeneration = 0;
        final List <File> files = Files.find(directory.toPath(), 1, ((path, basicFileAttributes)
                -> basicFileAttributes.isRegularFile()
                        && FILE_NAME_PATTERN.matcher(path.getFileName().toString()).find()
                        && path.getFileName().toString().endsWith(SUFFIX_DAT)))
                .map(Path::toFile)
                .collect(Collectors.toList());
        for(final File curFile: files) {
            final Path path = curFile.toPath();
            final long currGeneration = GenerationUtils.fromPath(path);
            ssTables.put(currGeneration, new SSTable(path.toFile()));
            maxGeneration = Math.max(currGeneration, maxGeneration);
        }
        maxGeneration = maxGeneration + 1;
        memoryTablePool = new MemoryTablePool(flushLimit, maxGeneration);
        flusherThread = new Thread(new FlusherTask());
        flusherThread.start();
        scheduledExecutorService = Executors.newScheduledThreadPool(2);
        scheduledExecutorService.scheduleAtFixedRate(new CompactionTask(), 2000, 2000, TimeUnit.MILLISECONDS);
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
        final List<Iterator<Cluster>> iters = new ArrayList<>();
        for (final SSTable ssTable : this.ssTables.descendingMap().values()) {
            iters.add(ssTable.iterator(from));
        }

        iters.add(memoryTablePool.iterator(from));
        final Iterator<Cluster> clusterIterator = Iters.collapseEquals(
                Iterators.mergeSorted(iters, Cluster.COMPARATOR),
                Cluster::getKey
        );
        return Iterators.filter(
                clusterIterator, cluster -> {
                    assert cluster != null;
                    return !cluster.getClusterValue().isTombstone();
                }
        );
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
            scheduledExecutorService.shutdownNow();
            flusherThread.join();
        } catch (InterruptedException e) {
           Thread.currentThread().interrupt();
        }
    }

    private void compact(long value) throws IOException {
        logger.info("Prepare to compact...");
        for(final SSTable ssTable : ssTables.values()) {
            Files.delete(ssTable.getTable().toPath());
        }
        ssTables = new ConcurrentSkipListMap<>();
        ssTables.put(value-1 , new SSTable(new File(directory, FILE_NAME + --value + SUFFIX_DAT)));
    }

    private void flush(final long generation, final Iterator <Cluster> data) throws IOException {
        final long startFlushTime = System.currentTimeMillis();
        logger.info("Flush start in: " + startFlushTime + " with generation: " + generation);

        if(data.hasNext()) {
            final File tmp = new File(directory, FILE_NAME + generation + SUFFIX_TMP);
            SSTable.writeToFile(data, tmp);
            final File database = new File(directory, FILE_NAME + generation + SUFFIX_DAT);
            Files.move(tmp.toPath(), database.toPath(), StandardCopyOption.ATOMIC_MOVE);
            ssTables.put(generation, new SSTable(database));
        }
        logger.info("Flush end in: " + System.currentTimeMillis() + " with generation: " + generation);
        logger.info("Estimated time: " + (System.currentTimeMillis() - startFlushTime));
    }

    private final class FlusherTask implements Runnable {

        @Override
        public void run() {
            boolean poisonReceived = false;
            while (!Thread.currentThread().isInterrupted() && !poisonReceived) {
                TableToFlush tableToFlush;
                try {
                    logger.info("Prepare to flush in flusher task: " + this.toString());
                    tableToFlush = memoryTablePool.tableToFlush();
                    poisonReceived = tableToFlush.isPoisonPills();
                    flush(tableToFlush.getGeneration(), tableToFlush.getTable().iterator(SMALLEST_KEY));
                    memoryTablePool.flushed(tableToFlush.getGeneration());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    logger.info("IO Error" + e.getMessage());
                }
            }
        }
    }

    private final class CompactionTask implements Runnable {

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                if(ssTables.descendingMap().values().size() > compactLimit) {
                    try {
                        logger.info("Prepare to compaction in compaction task : " + this.toString());
                        final long generation = memoryTablePool.getGeneration();
                        flush(generation, clusterIterator(SMALLEST_KEY));
                        compact(generation);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}