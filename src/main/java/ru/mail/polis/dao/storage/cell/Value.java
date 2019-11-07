package ru.mail.polis.dao.storage.cell;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.utils.BytesUtils;

import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.OptionalLong;

import static ru.mail.polis.service.rest.RestController.TIMESTAMP_HEADER;

public final class Value implements Comparable<Value> {

    private static final Value ABSENT = new Value(null, State.ABSENT, -1);

    private final ByteBuffer data;
    private final long timestamp;
    private final State state;

    /**
     * Persistence cell value.
     *
     * @param data      is the data of Value
     * @param state     is state of current Value.
     * @param timestamp is time witch this value is written
     */
    public Value(final ByteBuffer data,
                 @NotNull final State state,
                 final long timestamp) {
        this.data = data;
        this.state = state;
        this.timestamp = timestamp;
    }

    /**
     * Alive value (cell) in storage.
     *
     * @param data is data of value
     */
    public static Value of(@NotNull final ByteBuffer data) {
        return new Value(data.duplicate(),
                State.PRESENT,
                System.currentTimeMillis());
    }

    /**
     * Removed value (cell) in storage.
     */
    public static Value deadCluster() {
        return new Value(
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
    public static Value present(
            @NotNull final ByteBuffer data,
            final long timestamp) {
        return new Value(
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
    public static Value removed(final long timestamp) {
        return new Value(
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
    public int compareTo(@NotNull final Value o) {
        return -Long.compare(timestamp, o.timestamp);
    }

    public static Value absent() {
        return ABSENT;
    }

    public enum State {
        ABSENT,
        PRESENT,
        REMOVED
    }

    /**
     * Merge cells and get latest value from collection.
     *
     * @param values is collection from witch we should be get value
     */
    @NotNull
    public static Value merge(@NotNull final Collection<Value> values) {
        return values.stream()
                .filter(clusterValue -> clusterValue.getState() != Value.State.ABSENT)
                .max(Comparator.comparingLong(Value::getTimestamp))
                .orElseGet(Value::absent);
    }

    /**
     * Get value from Cell iterator .
     *
     * @param cells is iterator of cells
     * @param key   is key by we get data and merge
     */
    @NotNull
    public static Value valueOf(@NotNull final Iterator<Cell> cells,
                                 @NotNull final ByteBuffer key) {
        if (!cells.hasNext()) {
            return Value.absent();
        }

        final Cell cell = cells.next();

        if (!cell.getKey().equals(key)) {
            return Value.absent();
        }

        final long timestamp = cell.getValue().getTimestamp();
        final ByteBuffer value = cell.getValue().getData();
        if (value == null) {
            return Value.removed(timestamp);
        } else {
            return Value.present(value, timestamp);
        }
    }

    /**
     * Get CellValue from response.
     *
     * @param response response from witch we should be get value.
     */
    @NotNull
    public static Value fromHttpResponse(@NotNull final HttpResponse<byte[]> response) {
        final OptionalLong timestampOptional = response.headers().firstValueAsLong(TIMESTAMP_HEADER);
        final int statusCode = response.statusCode();

        if(statusCode == 200) {
            if(timestampOptional.isEmpty()) {
                throw new IllegalArgumentException("Timestamp must be not empty if status code is 200!");
            }
            final long ts = timestampOptional.getAsLong();
            final byte[] body = response.body();
            final ByteBuffer data = ByteBuffer.wrap(body);
            return Value.present(data, ts);
        } else {
            if(timestampOptional.isEmpty()) {
                return Value.absent();
            }
            final long ts = timestampOptional.getAsLong();
            return Value.removed(ts);
        }
    }

}
