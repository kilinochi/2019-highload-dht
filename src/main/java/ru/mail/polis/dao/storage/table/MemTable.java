package ru.mail.polis.dao.storage.table;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.storage.cluster.Cluster;
import ru.mail.polis.dao.storage.cluster.ClusterValue;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public final class MemTable implements Table {

    private final NavigableMap<ByteBuffer, ClusterValue> storage = new ConcurrentSkipListMap<>();
    private final NavigableMap<ByteBuffer, ClusterValue> unmodifiable = Collections.unmodifiableNavigableMap(storage);
    private AtomicLong tableSizeInBytes = new AtomicLong();


    /**
     * Get data as Iterator from in-memory storage by key.
     *
     * @param from is the label which we can find data
     *
     * */
    @NotNull
    @Override
    public final Iterator<Cluster> iterator(@NotNull final ByteBuffer from) {
        /*return Iterators.transform(storage.tailMap(from)
                        .entrySet().iterator(),
                e -> {
                    assert e != null;
                    return new Cluster(e.getKey(), e.getValue(), generation);
                });*/
        return Iterators.transform(unmodifiable.tailMap(from)
                        .entrySet()
                        .iterator(),
                new Function<Map.Entry<ByteBuffer, ClusterValue>, Cluster>() {
                    @Nullable
                    @Override
                    public Cluster apply(Map.@Nullable Entry<ByteBuffer, ClusterValue> input) {
                        return Cluster.of(input.getKey(), input.getValue());
                    }
                });
    }

    /**
     * Insert new Value to storage.
     *
     * @param key   is the label which we can find data
     * @param value is the data
     **/
    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        final ClusterValue prev = storage.put(key, ClusterValue.of(value));
        if (prev == null) {
            tableSizeInBytes.addAndGet(key.remaining() + value.remaining());
        } else if (prev.isTombstone()) {
            tableSizeInBytes.addAndGet(value.remaining());
        } else {
            tableSizeInBytes.addAndGet(value.remaining() - prev.getData().remaining());
        }
    }

    /**
     * Delete Value from storage by key.
     *
     * @param key is the label which we can find data
     *            and delete data from storage
     */
    @Override
    public void remove(@NotNull final ByteBuffer key) {
        final ClusterValue prev = storage.put(key, ClusterValue.deadCluster());
        if (prev == null) {
            tableSizeInBytes.addAndGet(key.remaining());
        } else if (!prev.isTombstone()) {
            tableSizeInBytes.addAndGet(-prev.getData().remaining());
        }
    }

    @Override
    public long size() {
        return tableSizeInBytes.get();
    }
}