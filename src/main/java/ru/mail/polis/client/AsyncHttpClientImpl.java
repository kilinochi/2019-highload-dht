package ru.mail.polis.client;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.storage.cell.Value;
import ru.mail.polis.utils.ConstUtils;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;

public final class AsyncHttpClientImpl implements AsyncHttpClient {

    private static final String ENTITY_PATH_ID = "/v0/entity?id=";

    private final HttpClient client;

    AsyncHttpClientImpl() {
        client = HttpClient
                .newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .build();
    }

    @Override
    public CompletableFuture<Void> upsert(@NotNull final byte[] value,
                                          @NotNull final String id,
                                          @NotNull final String url) {
        final HttpRequest httpRequest = builder(id, url).PUT(ofBytes(value)).build();
        return sendAsyncRequest(httpRequest);
    }

    @Override
    public CompletableFuture<Void> delete(@NotNull final String id,
                                          @NotNull final String url) {
        final HttpRequest httpRequest = builder(id, url).DELETE().build();
        return sendAsyncRequest(httpRequest);
    }

    @Override
    public CompletableFuture<Value> get(@NotNull final String id,
                                        @NotNull final String url) {
        final HttpRequest httpRequest = builder(id, url).GET().build();
        return client.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(Value::fromHttpResponse);
    }

    private HttpRequest.Builder builder(@NotNull final String id,
                                        @NotNull final String url) {
        return HttpRequest.newBuilder()
                .uri(URI.create(url + ENTITY_PATH_ID + id))
                .header(ConstUtils.PROXY_HEADER_NAME, ConstUtils.PROXY_HEADER_VALUE)
                .version(HttpClient.Version.HTTP_2);
    }

    private HttpRequest.BodyPublisher ofBytes(@NotNull final byte[] body) {
        return HttpRequest.BodyPublishers.ofByteArray(body);
    }

    private CompletableFuture<Void> sendAsyncRequest(@NotNull final HttpRequest httpRequest) {
        return client.sendAsync(httpRequest, HttpResponse.BodyHandlers.discarding())
                .thenApply(HttpResponse::body);
    }
}
