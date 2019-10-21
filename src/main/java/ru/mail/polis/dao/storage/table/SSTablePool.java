package ru.mail.polis.dao.storage.table;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.storage.cluster.Cluster;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class SSTablePool implements Table {

    private NavigableMap <Long, SSTable> ssTables = new ConcurrentSkipListMap<>();
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicLong generation = new AtomicLong();

    public SSTablePool(@NotNull final File directory,
                       final long maxGeneration) {

    }

    @Override
    public long size() {
        return 0;
    }

    @NotNull
    @Override
    public Iterator<Cluster> iterator(@NotNull ByteBuffer from) {

        return null;
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) {

    }

    @Override
    public void remove(@NotNull ByteBuffer key) {

    }

    @Override
    public long generation() {
        return 0;
    }

    public void compact() {

    }
}
