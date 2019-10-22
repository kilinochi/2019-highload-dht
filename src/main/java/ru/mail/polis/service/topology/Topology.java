package ru.mail.polis.service.topology;

import org.jetbrains.annotations.NotNull;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.util.Set;

@ThreadSafe
public interface Topology<T extends Node> {
    boolean isMe(@NotNull final T node);

    @NotNull
    T primaryFor(@NotNull final ByteBuffer key);

    @NotNull
    Set<T> all();
}
