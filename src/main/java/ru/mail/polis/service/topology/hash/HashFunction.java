package ru.mail.polis.service.topology.hash;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public interface HashFunction {
    long hash(@NotNull final ByteBuffer key);
}
