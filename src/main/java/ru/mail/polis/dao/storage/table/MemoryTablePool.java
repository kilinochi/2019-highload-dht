package ru.mail.polis.dao.storage.table;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.Iters;
import ru.mail.polis.dao.storage.LSMDao;
import ru.mail.polis.dao.storage.cell.Cell;
import ru.mail.polis.utils.IteratorUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Collection;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class MemoryTablePool implements Table, Closeable {

    private volatile MemTable currentMemoryTable;
    private final NavigableMap<Long, Table> pendingToFlushTables;
    private final BlockingQueue<FlushTable> flushingQueue;
    private long generation;

    private final long flushLimit;
    private final AtomicBoolean stop = new AtomicBoolean();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Pool of mem table to flush.
     *
     * @param flushLimit      is the limit above which we flushing mem table
     * @param startGeneration is the start of generation
     */
    public MemoryTablePool(final long flushLimit, final long startGeneration) {
        this.flushLimit = flushLimit;
        this.generation = startGeneration;
        this.currentMemoryTable = new MemTable(generation);
        this.pendingToFlushTables = new TreeMap<>();
        this.flushingQueue = new ArrayBlockingQueue<>(2);
    }

    @Override
    public long size() {
        lock.readLock().lock();
        try {
            long size = currentMemoryTable.size();
            for (final Map.Entry<Long, Table> table : pendingToFlushTables.entrySet()) {
                size = size + table.getValue().size();
            }
            return size;
        } finally {
            lock.readLock().unlock();
        }
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(final @NotNull ByteBuffer from) {
        final Collection<Iterator<Cell>> iterators;
        lock.readLock().lock();
        try {
            iterators = new ArrayList<>(pendingToFlushTables.size() + 1);
            iterators.add(currentMemoryTable.iterator(from));
            for (final Table table : pendingToFlushTables.descendingMap().values()) {
                iterators.add(table.iterator(from));
            }
        } finally {
            lock.readLock().unlock();
        }
        final Iterator<Cell> merged = Iterators.mergeSorted(iterators, Cell.COMPARATOR);
        final Iterator<Cell> withoutEquals = Iters.collapseEquals(merged, Cell::getKey);

        return Iterators.filter(
                withoutEquals,
                input -> input.getCellValue().getData() != null
        );
    }

    @Override
    public void upsert(final @NotNull ByteBuffer key, final @NotNull ByteBuffer value) {
        if (stop.get()) {
            throw new IllegalStateException("Already stopped!");
        }
        lock.readLock().lock();
        try {
            currentMemoryTable.upsert(key, value);
        } finally {
            lock.readLock().unlock();
        }
        enqueueFlush();
    }

    @Override
    public void remove(final @NotNull ByteBuffer key) {
        if (stop.get()) {
            throw new IllegalStateException("Already stopped!");
        }
        lock.readLock().lock();
        try {
            currentMemoryTable.remove(key);
        } finally {
            lock.readLock().unlock();
        }
        enqueueFlush();
    }

    /**
     * Return current generation of Pool.
     */
    @Override
    public long generation() {
        lock.readLock().lock();
        try {
            return generation;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Take from queue table.
     */
    public FlushTable tableToFlush() throws InterruptedException {
        return flushingQueue.take();
    }

    /**
     * Mark mem table as flushed and remove her from map storage of tables.
     *
     * @param generation is key by which we remove table from storage
     */
    public void flushed(final long generation) {
        lock.writeLock().lock();
        try {
            pendingToFlushTables.remove(generation);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Compact values from all tables with current table.
     *
     * @param sstables is all tables from disk storage
     * @param directory is current directory for flush
     * @param generation is generation for new SSTable
     */
    public void compact(@NotNull final NavigableMap<Long, SSTable> sstables,
                        @NotNull final File directory,
                        final long generation) throws IOException {
        final Iterator<Cell> data;
        lock.readLock().lock();
        try {
            data = IteratorUtils.data(currentMemoryTable, sstables, LSMDao.EMPTY_BUFFER);
        } finally {
            lock.readLock().unlock();
        }
        compaction(data, directory, sstables ,generation);
    }

    private void compaction(@NotNull final Iterator<Cell> data,
                            @NotNull final File directory,
                            @NotNull final NavigableMap<Long, SSTable> ssTables,
                            final long generation) throws IOException {
        final File ssTableFileTmp = new File(directory, LSMDao.FILE_NAME + generation + LSMDao.SUFFIX_TMP);
        lock.writeLock().lock();
        try {
            SSTable.writeToFile(data, ssTableFileTmp);
            for(final SSTable ssTable : ssTables.descendingMap().values()) {
                Files.delete(ssTable.getTable().toPath());
            }
            ssTables.clear();
            final File ssTableFile = new File(directory, LSMDao.FILE_NAME + generation + LSMDao.SUFFIX_DAT);
            Files.move(ssTableFileTmp.toPath(), ssTableFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
            ssTables.put(generation, new SSTable(ssTableFile, generation));
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void enqueueFlush() {
        if(currentMemoryTable.size() > flushLimit) {
            FlushTable flushTable = null;
            lock.writeLock().lock();
            try {
                if (currentMemoryTable.size() > flushLimit) {
                    flushTable = new FlushTable(generation,
                            currentMemoryTable.iterator(LSMDao.EMPTY_BUFFER),
                            false);
                    pendingToFlushTables.put(generation, currentMemoryTable);
                    generation = generation + 1;
                    currentMemoryTable = new MemTable(generation);
                }
            } finally {
                lock.writeLock().unlock();
            }
            if (flushTable != null) {
                try {
                    flushingQueue.put(flushTable);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (!stop.compareAndSet(false, true)) {
            return;
        }
        FlushTable flushTable;
        lock.writeLock().lock();
        try {
            flushTable = new FlushTable(generation, currentMemoryTable.iterator(LSMDao.EMPTY_BUFFER), true);
        } finally {
            lock.writeLock().unlock();
        }

        try {
            flushingQueue.put(flushTable);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
