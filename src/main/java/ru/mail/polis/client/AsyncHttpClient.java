package ru.mail.polis.client;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.storage.cell.Value;

import java.util.concurrent.CompletableFuture;

public interface AsyncHttpClient {
    CompletableFuture<Void> upsert(@NotNull final byte[] value, @NotNull final String id, @NotNull final String url);

    CompletableFuture<Void> delete(@NotNull final String id, @NotNull final String url);
    
    CompletableFuture<Value> get(@NotNull final String id, @NotNull final String url);

    static AsyncHttpClient create() {
            return new AsyncHttpClientImpl();
    }
}
