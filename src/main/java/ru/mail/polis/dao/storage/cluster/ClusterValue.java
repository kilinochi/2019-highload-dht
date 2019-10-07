package ru.mail.polis.dao.storage.cluster;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.storage.utils.TimeUtils;

import java.nio.ByteBuffer;

public final class ClusterValue implements Comparable<ClusterValue> {

    private final ByteBuffer data;
    private final long timestamp;
    private final boolean tombstone;

    public static ClusterValue of(@NotNull final ByteBuffer data) {
        return new ClusterValue(data.duplicate(), TimeUtils.getTimeNanos(), false);
    }

    public static ClusterValue deadCluster() {
        return new ClusterValue(null, TimeUtils.getTimeNanos(), true);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public ByteBuffer getData() {
        return data.asReadOnlyBuffer();
    }

    public ClusterValue(final ByteBuffer data, final long timestamp, final boolean isDead) {
        this.data = data;
        this.timestamp = timestamp;
        this.tombstone = isDead;
    }

    public boolean isTombstone() {
        return tombstone;
    }

    @Override
    public int compareTo(@NotNull final ClusterValue o) {
        return -Long.compare(timestamp, o.timestamp);
    }
}
