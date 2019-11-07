package ru.mail.polis.client;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.storage.cell.Value;
import ru.mail.polis.utils.BytesUtils;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public final class AsyncHttpClientImpl implements AsyncHttpClient {

    private static final Logger logger = LoggerFactory.getLogger(AsyncHttpClientImpl.class);
    private static final String ENTITY_PATH_ID = "/v0/entity?id=";

    private final HttpClient client;
    private final String url;

    public AsyncHttpClientImpl(@NotNull final String url) {
        client = HttpClient
                .newBuilder()
                .build();
        this.url = url;
    }

    @Override
    public CompletableFuture<Void> upsert(@NotNull ByteBuffer key,
                                          @NotNull ByteBuffer value,
                                          @NotNull final String id) {
        logger.info("ASYNC REQUEST PUT with uri = {}, id = {} ", url, id);
        final HttpRequest httpRequest = builder(id).PUT(ofBytes(BytesUtils.body(value))).build();
        return client.sendAsync(httpRequest, HttpResponse.BodyHandlers.discarding())
                .thenApply(HttpResponse::body);
    }

    @Override
    public CompletableFuture<Void> delete(@NotNull ByteBuffer key,
                                          @NotNull final String id) {
        logger.info("ASYNC REQUEST DELETE with uri = {}, id = {} ", url, id);
        final HttpRequest httpRequest = builder(id).DELETE().build();
        return client.sendAsync(httpRequest, HttpResponse.BodyHandlers.discarding())
                .thenApply(HttpResponse::body);
    }

    @Override
    public CompletableFuture<Value> get(@NotNull ByteBuffer key,
                                        @NotNull final String id) {
        final HttpRequest httpRequest = builder(id).GET().build();
        logger.info("ASYNC REQUEST GET with uri = {}, id = {} ", url, id);
        return client.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(Value::fromHttpResponse);
    }

    private HttpRequest.Builder builder(@NotNull final String id) {
        return HttpRequest.newBuilder()
                .uri(URI.create(url + ENTITY_PATH_ID + id))
                .timeout(Duration.ofMillis(150))
                .header("X-OK-Proxy", "True");
    }

    private HttpRequest.BodyPublisher ofBytes(@NotNull final byte[] body) {
        return HttpRequest.BodyPublishers.ofByteArray(body);
    }
}
