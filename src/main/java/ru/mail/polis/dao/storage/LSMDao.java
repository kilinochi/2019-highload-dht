package ru.mail.polis.dao.storage;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.Iters;
import ru.mail.polis.dao.storage.table.MemoryTablePool;
import ru.mail.polis.dao.storage.table.SSTable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class LSMDao implements DAO {

    private static final String SUFFIX_DAT = ".dat";
    private static final String SUFFIX_TMP = ".tmp";
    private static final String FILE_NAME = "SSTable_";
    private static final Pattern WATCH_FILE_NAME = Pattern.compile(FILE_NAME);
    private static final ByteBuffer SMALLEST_KEY = ByteBuffer.allocate(0);

    private final File directory;
    private final long compactLimit;
    private final long flushLimit;
    private MemoryTablePool memoryTablePool;
    private List<SSTable> ssTables;


    /**
     * Creates persistence Dao based on LSMTree.
     *
     * @param flushLimit is the limit upon reaching which we write data in disk
     * @param directory  is the base directory, where contains our database
     * @throws IOException of an I/O error occurred
     *
     * */
    public LSMDao(@NotNull final File directory, final long compactLimit, final long flushLimit) throws IOException {
        this.compactLimit = compactLimit;
        this.flushLimit = flushLimit;
        this.directory = directory;
        ssTables = new ArrayList<>();
        Files.walkFileTree(directory.toPath(), EnumSet.noneOf(FileVisitOption.class), 1, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path path, final BasicFileAttributes attrs)
                    throws IOException {
                final Matcher matcher = WATCH_FILE_NAME.matcher(path.toString());
                if (path.toString().endsWith(SUFFIX_DAT) && matcher.find()) {
                    final long currentGeneration = Generation.fromPath(path);
                    generation = Math.max(generation, currentGeneration);
                    ssTables.add(new SSTable(path.toFile(), currentGeneration));
                }
                return FileVisitResult.CONTINUE;
            }
        });
        generation++;
        memTable = new MemoryTablePool(generation);
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

        iters.add(memTable.iterator(from));
        final Iterator<Cluster> clusterIterator = Iters.collapseEquals(
                Iterators.mergeSorted(iters, Cluster.COMPARATOR),
                Cluster::getKey
        );
        final Iterator<Cluster> alive = Iterators.filter(
                clusterIterator, cluster -> {
                    assert cluster != null;
                    return !cluster.getClusterValue().isTombstone();
                }
        );
        return alive;
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (ssTables.size() > compactLimit) {
            compact();
        }
        if (memTable.size() >= flushLimit) {
            flush(memTable.iterator(SMALLEST_KEY));
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (ssTables.size() > compactLimit) {
            compact();
        }
        if (memTable.size() >= flushLimit) {
            flush(memTable.iterator(SMALLEST_KEY));
        }
    }

    @Override
    public void close() throws IOException {
        if (memTable.size() > 0) {
            flush(memTable.iterator(SMALLEST_KEY));
        }
        if (ssTables.size() > compactLimit) {
            compact();
        }
    }

    @Override
    public void compact() throws IOException {
        final Iterator<Cluster> data = clusterIterator(SMALLEST_KEY);
        flush(data);
        ssTables.forEach(ssTable -> {
            try {
                Files.delete(ssTable.getTable().toPath());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        ssTables = new ArrayList<>();
        ssTables.add(new SSTable(new File(directory, FILE_NAME + --generation + SUFFIX_DAT), --generation));
    }

    private void flush(@NotNull final Iterator <Cluster> data) throws IOException {
        final File tmp = new File(directory, FILE_NAME + generation + SUFFIX_TMP);
        SSTable.writeToFile(data, tmp);
        final File dest = new File(directory, FILE_NAME + generation + SUFFIX_DAT);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        generation++;
        memTable = new MemoryTablePool(generation);
    }
}
