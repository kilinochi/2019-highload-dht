package ru.mail.polis.dao.storage.cluster;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class ClusterValue implements Comparable<ClusterValue> {

    private final ByteBuffer data;
    private final long timestamp;
    private final boolean tombstone;

    /**
     * Persistence cluster value.
     *
     * @param data is the data of Value
     * @param timestamp is time witch this value is written
     * @param isDead is flag of alive or not this value
     *
     **/

    public ClusterValue(final ByteBuffer data, final long timestamp, final boolean isDead) {
        this.data = data;
        this.timestamp = timestamp;
        this.tombstone = isDead;
    }

    public static ClusterValue of(@NotNull final ByteBuffer data) {
        return new ClusterValue(data.duplicate(), System.currentTimeMillis(), false);
    }

    public static ClusterValue deadCluster() {
        return new ClusterValue(null, System.currentTimeMillis(), true);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public ByteBuffer getData() {
        return data;
    }

    public boolean isTombstone() {
        return tombstone;
    }

    @Override
    public int compareTo(@NotNull final ClusterValue o) {
        return -Long.compare(timestamp, o.timestamp);
    }
}
