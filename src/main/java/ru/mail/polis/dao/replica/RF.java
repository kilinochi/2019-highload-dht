package ru.mail.polis.dao.replica;

import com.google.common.base.Splitter;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public final class RF {
    private final int ack;
    private final int from;


    /**
     * Replica factor is factor on how many copies we need for CRUD-operation.
     *
     * @param ack  - is how many response we wait for create one response for client
     * @param from from how many nodes we to attract for CRUD operation
     */
    public RF(final int ack, final int from) {
        this.ack = ack;
        this.from = from;
    }

    /**
     * Parse request and create replica factor.
     *
     * @param value is request param from request
     */
    @NotNull
    public static RF of(@NotNull final String value) {
        final List<String> values = Splitter.on('/').splitToList(value);
        if (values.size() != 2) {
            throw new IllegalArgumentException("Wrong replica factor:" + value);
        }
        return new RF(Integer.parseInt(values.get(0)), Integer.parseInt(values.get(1)));
    }

    public int getFrom() {
        return from;
    }

    public int getAck() {
        return ack;
    }
}
