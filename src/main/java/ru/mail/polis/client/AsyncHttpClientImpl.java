package ru.mail.polis.client;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.utils.ConstUtils;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public final class AsyncHttpClientImpl implements AsyncHttpClient {

    private static final Logger logger = LoggerFactory.getLogger(AsyncHttpClientImpl.class);
    private static final String ENTITY_PATH_ID = "/v0/entity?id=";

    private final HttpClient client;

    AsyncHttpClientImpl() {
        client = HttpClient
                .newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .build();
    }

    @Override
    public CompletableFuture<HttpResponse<Void>> upsert(@NotNull byte[] value, @NotNull String id, @NotNull final String url) {
        logger.info("ASYNC REQUEST PUT with uri = {}, id = {} ", url, id);
        final HttpRequest httpRequest = builder(id, url).PUT(ofBytes(value)).build();
        return client.sendAsync(httpRequest, HttpResponse.BodyHandlers.discarding());
    }

    @Override
    public CompletableFuture<HttpResponse<Void>> delete(@NotNull final String id, @NotNull final String url) {
        logger.info("ASYNC REQUEST DELETE with uri = {}, id = {} ", url, id);
        final HttpRequest httpRequest = builder(id, url).DELETE().build();
        return client.sendAsync(httpRequest, HttpResponse.BodyHandlers.discarding());
    }

    @Override
    public CompletableFuture<HttpResponse<byte[]>> get(@NotNull final String id, @NotNull final String url) {
        final HttpRequest httpRequest = builder(id, url).GET().build();
        logger.info("ASYNC REQUEST GET with uri = {}, id = {} ", url, id);
        return client.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofByteArray());
    }

    private HttpRequest.Builder builder(@NotNull final String id, @NotNull final String url) {
        return HttpRequest.newBuilder()
                .uri(URI.create(url + ENTITY_PATH_ID + id))
                .timeout(Duration.ofMillis(150))
                .header(ConstUtils.PROXY_HEADER_NAME, ConstUtils.PROXY_HEADER_VALUE);
    }

    private HttpRequest.BodyPublisher ofBytes(@NotNull final byte[] body) {
        return HttpRequest.BodyPublishers.ofByteArray(body);
    }
}
