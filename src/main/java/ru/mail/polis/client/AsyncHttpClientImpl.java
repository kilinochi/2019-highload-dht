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
    private final String url;

    public AsyncHttpClientImpl(@NotNull final String url,
                               @NotNull final Executor executor) {
        client = HttpClient
                .newBuilder()
                .executor(executor)
                .version(HttpClient.Version.HTTP_2)
                .build();
        this.url = url;
    }

    @Override
    public CompletableFuture<HttpResponse<Void>> upsert(@NotNull byte[] value, @NotNull String id) {
        logger.info("ASYNC REQUEST PUT with uri = {}, id = {} ", url, id);
        final HttpRequest httpRequest = builder(id).PUT(ofBytes(value)).build();
        return client.sendAsync(httpRequest, HttpResponse.BodyHandlers.discarding());
    }

    @Override
    public CompletableFuture<HttpResponse<Void>> delete(@NotNull final String id) {
        logger.info("ASYNC REQUEST DELETE with uri = {}, id = {} ", url, id);
        final HttpRequest httpRequest = builder(id).DELETE().build();
        return client.sendAsync(httpRequest, HttpResponse.BodyHandlers.discarding());
    }

    @Override
    public CompletableFuture<HttpResponse<byte[]>> get(@NotNull final String id) {
        final HttpRequest httpRequest = builder(id).GET().build();
        logger.info("ASYNC REQUEST GET with uri = {}, id = {} ", url, id);
        return client.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofByteArray());
    }

    private HttpRequest.Builder builder(@NotNull final String id) {
        return HttpRequest.newBuilder()
                .uri(URI.create(url + ENTITY_PATH_ID + id))
                .timeout(Duration.ofMillis(150))
                .header(ConstUtils.PROXY_HEADER_NAME, ConstUtils.PROXY_HEADER_VALUE);
    }

    private HttpRequest.BodyPublisher ofBytes(@NotNull final byte[] body) {
        return HttpRequest.BodyPublishers.ofByteArray(body);
    }
}
