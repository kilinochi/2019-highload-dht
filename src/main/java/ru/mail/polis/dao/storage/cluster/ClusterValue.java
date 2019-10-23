package ru.mail.polis.dao.storage.cluster;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class ClusterValue implements Comparable<ClusterValue> {

    private static final ClusterValue ABSENT = new ClusterValue(null, State.ABSENT, -1);

    private final ByteBuffer data;
    private final long timestamp;
    private final State state;

    /**
     * Persistence cluster value.
     *
     * @param data is the data of Value
     * @param state is state of current Value.
     * @param timestamp is time witch this value is written
     */
    public ClusterValue(final ByteBuffer data,
                        @NotNull final State state,
                        final long timestamp) {
        this.data = data;
        this.state = state;
        this.timestamp = timestamp;
    }

    /**
     * Alive cluster value (cell) in storage.
     * @param data is data of value
     */
    public static ClusterValue of(@NotNull final ByteBuffer data) {
        return new ClusterValue(data.duplicate(),
                State.PRESENT,
                System.currentTimeMillis());
    }

    /**
     * Removed value (cell) in storage.
     */
    public static ClusterValue deadCluster() {
        return new ClusterValue(
                null,
                State.REMOVED,
                System.currentTimeMillis());
    }
    /**
     * Present (alive) value witch we want to read by timestamp.
     * @param data us data in this value.
     * @param timestamp is timestamp in this value.
     */
    public static ClusterValue present(
            @NotNull final ByteBuffer data,
            final long timestamp) {
        return new ClusterValue(
                data,
                State.PRESENT,
                timestamp
        );
    }

    public static ClusterValue removed(final long timestamp) {
        return new ClusterValue(
                null,
                State.REMOVED,
                timestamp
        );
    }

    public long getTimestamp() {
        return timestamp;
    }

    public ByteBuffer getData() {
        return data;
    }

    public State getState() {
        return state;
    }

    @Override
    public int compareTo(@NotNull final ClusterValue o) {
        return -Long.compare(timestamp, o.timestamp);
    }

    public static ClusterValue absent() {
        return ABSENT;
    }

    public enum State {
        ABSENT,
        PRESENT,
        REMOVED
    }
}
