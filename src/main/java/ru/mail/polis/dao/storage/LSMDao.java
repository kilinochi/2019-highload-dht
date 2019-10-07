package ru.mail.polis.dao.storage;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.Iters;
import ru.mail.polis.dao.storage.cluster.Cluster;
import ru.mail.polis.dao.storage.table.MemoryTablePool;
import ru.mail.polis.dao.storage.table.SSTable;
import ru.mail.polis.dao.storage.table.Table;
import ru.mail.polis.dao.storage.table.TableToFlush;
import ru.mail.polis.dao.storage.utils.GenerationUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class LSMDao implements DAO {

    private static final String SUFFIX_DAT = ".dat";
    private static final String SUFFIX_TMP = ".tmp";
    private static final String FILE_NAME = "SSTable_";
    private static final Pattern FILE_NAME_PATTERN = Pattern.compile(FILE_NAME);
    private static final ByteBuffer SMALLEST_KEY = ByteBuffer.allocate(0);

    private final File directory;
    private final long compactLimit;
    private final Thread flusherThread;
    private final MemoryTablePool memoryTablePool;

    private List<SSTable> ssTables;


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
        this.compactLimit = compactLimit;
        this.directory = directory;
        ssTables = new ArrayList<>();
        long maxGeneration = 0;
        final Collection <File> files
                = Files.find(directory.toPath(), 1, ((path, basicFileAttributes) -> basicFileAttributes.isRegularFile()
                        && FILE_NAME_PATTERN.matcher(path.getFileName().toString()).find()
                        && path.getFileName().toString().endsWith(SUFFIX_DAT)))
                .map(Path::toFile)
                .collect(Collectors.toList());
        for(final File curFile: files) {
            final Path path = curFile.toPath();
            final long currGeneration = GenerationUtils.fromPath(path);
            maxGeneration = Math.max(currGeneration, maxGeneration);
        }
        maxGeneration = maxGeneration + 1;
        memoryTablePool = new MemoryTablePool(flushLimit, maxGeneration);
        flusherThread = new Thread(new FlusherTask());
        flusherThread.start();
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
        for (final SSTable ssTable : this.ssTables) {
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
            flusherThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void compact() throws IOException {
       /* final Iterator<Cluster> data = clusterIterator(SMALLEST_KEY);
        flush(data);
        ssTables.forEach(ssTable -> {
            try {
                Files.delete(ssTable.getTable().toPath());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        ssTables = new ArrayList<>();
        ssTables.add(new SSTable(new File(directory, FILE_NAME + --generation + SUFFIX_DAT), --generation));*/
    }

    private void flush(final long generation, final Table table) throws IOException {
        Iterator <Cluster> data = table.iterator(SMALLEST_KEY);
        if(data.hasNext()) {
            final File tmp = new File(directory, FILE_NAME + generation + SUFFIX_TMP);
            SSTable.writeToFile(data, tmp);
            final File dest = new File(directory, FILE_NAME + generation + SUFFIX_DAT);
            Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        }
    }

    private class FlusherTask implements Runnable {

        @Override
        public void run() {
            boolean poisonReceived = false;
            while (!Thread.currentThread().isInterrupted() && !poisonReceived) {
                TableToFlush tableToFlush;
                try {
                    tableToFlush = memoryTablePool.tableToFlush();
                    poisonReceived = tableToFlush.isPoisonPills();
                    flush(tableToFlush.getGeneration(), tableToFlush.getTable());
                    memoryTablePool.flushed(tableToFlush.getGeneration());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    //error
                }
            }
        }
    }
}
