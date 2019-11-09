package ru.mail.polis.service.replica;

import com.google.common.base.Splitter;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public final class RF {
    private final int ack;
    private final int from;

    public RF(final int ack, final int from) {
        this.ack = ack;
        this.from = from;
    }

    public int getAck() {
        return ack;
    }

    public int getFrom() {
        return from;
    }

    @NotNull
    public static RF of(@NotNull final String value) {
        final List<String> values = Splitter.on('/').splitToList(value);
        if (values.size() != 2) {
            throw new IllegalArgumentException("Wrong replica factor:" + value);
        }
        return new RF(Integer.parseInt(values.get(0)), Integer.parseInt(values.get(1)));
    }
}