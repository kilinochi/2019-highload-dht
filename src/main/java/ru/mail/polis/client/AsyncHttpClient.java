package ru.mail.polis.client;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.storage.cell.Value;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public interface AsyncHttpClient {
    CompletableFuture<Void> upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value, @NotNull final String id);
    CompletableFuture<Void> delete(@NotNull final ByteBuffer key, @NotNull final String id);
    CompletableFuture<Value> get(@NotNull final ByteBuffer key, @NotNull final String id);
}
