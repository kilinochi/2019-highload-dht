package ru.mail.polis.client;

import org.jetbrains.annotations.NotNull;

import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;

public interface AsyncHttpClient {
    CompletableFuture<HttpResponse<Void>> upsert(@NotNull final byte[] value, @NotNull final String id);
    CompletableFuture<HttpResponse<Void>> delete(@NotNull final String id);
    CompletableFuture<HttpResponse<byte[]>> get(@NotNull final String id);
}
