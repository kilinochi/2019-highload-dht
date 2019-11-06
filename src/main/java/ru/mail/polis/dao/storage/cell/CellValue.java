package ru.mail.polis.dao.storage.cell;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class CellValue implements Comparable<CellValue> {

    private static final CellValue ABSENT = new CellValue(null, State.ABSENT, -1);

    private final ByteBuffer data;
    private final long timestamp;
    private final State state;

    /**
     * Persistence cluster value.
     *
     * @param data      is the data of Value
     * @param state     is state of current Value.
     * @param timestamp is time witch this value is written
     */
    public CellValue(final ByteBuffer data,
                     @NotNull final State state,
                     final long timestamp) {
        this.data = data;
        this.state = state;
        this.timestamp = timestamp;
    }

    /**
     * Alive cluster value (cell) in storage.
     *
     * @param data is data of value
     */
    public static CellValue of(@NotNull final ByteBuffer data) {
        return new CellValue(data.duplicate(),
                State.PRESENT,
                System.currentTimeMillis());
    }

    /**
     * Removed value (cell) in storage.
     */
    public static CellValue deadCluster() {
        return new CellValue(
                null,
                State.REMOVED,
                System.currentTimeMillis());
    }

    /**
     * Present (alive) value witch we want to read by timestamp.
     *
     * @param data      us data in this value.
     * @param timestamp is timestamp in this value.
     */
    public static CellValue present(
            @NotNull final ByteBuffer data,
            final long timestamp) {
        return new CellValue(
                data,
                State.PRESENT,
                timestamp
        );
    }

    /**
     * Removed (dead) value in storage.
     *
     * @param timestamp is timestamp of this value.
     */
    public static CellValue removed(final long timestamp) {
        return new CellValue(
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
    public int compareTo(@NotNull final CellValue o) {
        return -Long.compare(timestamp, o.timestamp);
    }

    public static CellValue absent() {
        return ABSENT;
    }

    public enum State {
        ABSENT,
        PRESENT,
        REMOVED
    }
}
