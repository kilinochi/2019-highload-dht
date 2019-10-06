package ru.mail.polis.dao.storage.table;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.Iters;
import ru.mail.polis.dao.storage.Cluster;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Iterator;
import java.util.Collection;
import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class MemoryTablePool implements Table, Closeable {

    private volatile MemTable currentTable;
    private NavigableMap<Long, Table> flushingTables;
    private BlockingQueue <TableToFlush> flushingQueue;
    private long generation;

    private final AtomicBoolean stop = new AtomicBoolean();
    private final long flushLimit;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public MemoryTablePool(final long flushLimit, final long startGeneration) {
        this.flushLimit = flushLimit;
        this.generation = startGeneration;
        this.currentTable = new MemTable();
        this.flushingTables = new TreeMap<>();
        this.flushingQueue = new ArrayBlockingQueue<>(2);
    }


    @Override
    public long size() {
        lock.readLock().lock();
        try {
            long size = currentTable.size();
            for (Map.Entry<Long, Table> table: flushingTables.entrySet()) {
                size = table.getValue().size();
            }
            return size;
        } finally {
           lock.readLock().unlock();
        }
    }


    @NotNull
    @Override
    public Iterator<Cluster> iterator(@NotNull ByteBuffer from) {
        lock.readLock().lock();
        final Collection<Iterator<Cluster>> iterators;
        try {
            iterators = new ArrayList<>(flushingTables.size() + 1);
            iterators.add(currentTable.iterator(from));
            for (final Table table : flushingTables.descendingMap().values()) {
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
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) {
        if(stop.get()) {
            throw new IllegalStateException("Already stopped!");
        }
        currentTable.upsert(key, value);
        enqueueFlush();
    }

    @Override
    public void remove(@NotNull ByteBuffer key) {
        if(stop.get()) {
            throw new IllegalStateException("Already stopped!");
        }
        currentTable.remove(key);
        enqueueFlush();
    }

    public TableToFlush tableToFlush() throws InterruptedException {
        return flushingQueue.take();
    }

    public void flushed(final long generation) {
        lock.writeLock().lock();
        try {
            flushingTables.remove(generation);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    private void enqueueFlush () {
        if(currentTable.size() > flushLimit) {
            lock.writeLock().lock();
            TableToFlush tableToFlush = null;
            try {
                if (currentTable.size() > flushLimit) {
                    tableToFlush = new TableToFlush(generation, currentTable);
                    generation = generation + 1;
                    currentTable = new MemTable();
                }
            } finally {
                lock.writeLock().unlock();
            }
            if(tableToFlush != null) {
                try {
                    flushingQueue.put(tableToFlush);
                } catch (InterruptedException e) {
                    e.printStackTrace();
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
            tableToFlush = new TableToFlush(generation, currentTable, true);
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