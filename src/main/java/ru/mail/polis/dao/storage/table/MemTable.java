package ru.mail.polis.dao.storage.table;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.storage.cell.Cell;
import ru.mail.polis.dao.storage.cell.CellValue;

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

    private final NavigableMap<ByteBuffer, CellValue> storage = new ConcurrentSkipListMap<>();
    private final NavigableMap<ByteBuffer, CellValue> unmodifiable = Collections.unmodifiableNavigableMap(storage);
    private final long generation;
    private final AtomicLong tableSizeInBytes = new AtomicLong();

    MemTable(final long generation) {
        this.generation = generation;
    }

    /**
     * Get data as Iterator from in-memory storage by key.
     * @param from is the label which we can find data
     */
    @NotNull
    @Override
    public final Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return Iterators.transform(unmodifiable.tailMap(from)
                        .entrySet()
                        .iterator(),
                new Function<Map.Entry<ByteBuffer, CellValue>, Cell>() {
                    @NotNull
                    @Override
                    public Cell apply(Map.@Nullable Entry<ByteBuffer, CellValue> input) {
                        return Cell.of(input.getKey(), input.getValue(), generation);
                    }
                });
    }

    /**
     * Insert new Value to storage.
     *
     * @param key   is the label which we can find data
     * @param value is the data
     */
    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        final CellValue prev = storage.put(key, CellValue.of(value));

        if (prev == null) {
            tableSizeInBytes.addAndGet(key.remaining() + value.remaining());
        } else if (prev.getState() == CellValue.State.REMOVED) {
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
        final CellValue prev = storage.put(key, CellValue.deadCluster());
        if (prev == null) {
            tableSizeInBytes.addAndGet(key.remaining());
        } else if (prev.getState() != CellValue.State.REMOVED) {
            tableSizeInBytes.addAndGet(-prev.getData().remaining());
        }
    }

    @Override
    public long generation() {
        return this.generation;
    }

    @Override
    public long size() {
        return tableSizeInBytes.get();
    }
}
