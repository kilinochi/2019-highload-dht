package ru.mail.polis.dao.storage.table;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.Iters;
import ru.mail.polis.dao.storage.cluster.Cluster;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
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
    private NavigableMap<Long, Table> pendingToFlushTables;
    private BlockingQueue <TableToFlush> flushingQueue;
    private long generation;

    private final long flushLimit;
    private final AtomicBoolean stop = new AtomicBoolean();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Pool of mem table to flush.
     *
     * @param flushLimit is the limit above which we flushing mem table
     * @param startGeneration is the start of generation
     **/

    public MemoryTablePool(final long flushLimit, final long startGeneration) {
        this.flushLimit = flushLimit;
        this.generation = startGeneration;
        this.currentMemoryTable = new MemTable();
        this.pendingToFlushTables = new TreeMap<>();
        this.flushingQueue = new ArrayBlockingQueue<>(2);
    }

    /**
    * return current generation of Pool.
     *
     * */

    public long getGeneration() {
        lock.readLock().lock();
        try {
            return generation;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long size() {
        lock.readLock().lock();
        try {
            long size = currentMemoryTable.size();
            for (Map.Entry<Long, Table> table: pendingToFlushTables.entrySet()) {
                size = size + table.getValue().size();
            }
            return size;
        } finally {
           lock.readLock().unlock();
        }
    }


    @NotNull
    @Override
    public Iterator<Cluster> iterator(final @NotNull ByteBuffer from) {
        lock.readLock().lock();
        final Collection<Iterator<Cluster>> iterators;
        try {
            iterators = new ArrayList<>(pendingToFlushTables.size() + 1);
            iterators.add(currentMemoryTable.iterator(from));
            for (final Table table : pendingToFlushTables.descendingMap().values()) {
                iterators.add(table.iterator(from));
            }
        } finally {
            lock.readLock().unlock();
        }
        final Iterator <Cluster> merged = Iterators.mergeSorted(iterators, Cluster.COMPARATOR);
        final Iterator <Cluster> withoutEquals = Iters.collapseEquals(merged, Cluster::getKey);

        return Iterators.filter(
                        withoutEquals,
                        input -> input.getClusterValue().getData() != null
        );
    }


    @Override
    public void upsert(final @NotNull ByteBuffer key, final @NotNull ByteBuffer value) {
        if(stop.get()) {
            throw new IllegalStateException("Already stopped!");
        }
        currentMemoryTable.upsert(key, value);
        enqueueFlush();
    }

    @Override
    public void remove(final @NotNull ByteBuffer key) {
        if(stop.get()) {
            throw new IllegalStateException("Already stopped!");
        }
        currentMemoryTable.remove(key);
        enqueueFlush();
    }

    public TableToFlush tableToFlush() throws InterruptedException {
        return flushingQueue.take();
    }

    /**
    * Mark mem table as flushed and remove her from map storage of tables.
    * @param generation is key by which we remove table from storage
    *
    * */

    public void flushed(final long generation) {
        lock.writeLock().lock();
        try {
            pendingToFlushTables.remove(generation);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    private void enqueueFlush() {
        if(currentMemoryTable.size() > flushLimit) {
            lock.writeLock().lock();
            TableToFlush tableToFlush = null;
            try {
                if (currentMemoryTable.size() > flushLimit) {
                    tableToFlush = new TableToFlush(generation, currentMemoryTable);
                    pendingToFlushTables.put(generation, currentMemoryTable);
                    generation = generation + 1;
                    currentMemoryTable = new MemTable();
                }
            } finally {
                lock.writeLock().unlock();
            }
            if(tableToFlush != null) {
                try {
                    flushingQueue.put(tableToFlush);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        if(!stop.compareAndSet(false, true)) {
            return;
        }
        lock.writeLock().lock();
        TableToFlush tableToFlush;
        try {
            tableToFlush = new TableToFlush(generation, currentMemoryTable, true);
        } finally {
            lock.writeLock().unlock();
        }

        try {
            flushingQueue.put(tableToFlush);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}