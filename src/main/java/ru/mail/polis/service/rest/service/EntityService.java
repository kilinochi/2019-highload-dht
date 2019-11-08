package ru.mail.polis.service.rest.service;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.client.AsyncHttpClient;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.storage.cell.Cell;
import ru.mail.polis.dao.storage.cell.Value;
import ru.mail.polis.promise.CompletablePromise;
import ru.mail.polis.service.topology.Topology;
import ru.mail.polis.service.topology.node.ServiceNode;
import ru.mail.polis.utils.FutureUtils;
import ru.mail.polis.utils.ResponseUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

public final class EntityService {

    private static final Logger logger = LoggerFactory.getLogger(EntityService.class);

    private final DAO dao;
    private final Topology<ServiceNode> topology;
    private final AsyncHttpClient client;
    private final ExecutorService serviceWorkers;

    public EntityService(@NotNull final DAO dao,
                         @NotNull final Topology<ServiceNode> topology) {
        this.dao = dao;
        this.topology = topology;
        this.client = AsyncHttpClient.create();
        serviceWorkers = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors() + 1,
                new ThreadFactoryBuilder().setNameFormat("entity-service-workers-%d").build());
    }

    public Response delete(
                @NotNull final String id,
                @NotNull final ByteBuffer key,
                final int acks,
                final int from,
                final boolean proxy) throws IOException {
        if(proxy) {
            dao.remove(key);
            return new Response(Response.ACCEPTED, Response.EMPTY);
        }
        final Collection<CompletableFuture<Void>> futures = new ConcurrentLinkedQueue<>();
        topology.replicas(from, key)
                .forEach(serviceNode -> {
                    if(topology.isMe(serviceNode)) {
                        CompletableFuture<Void> future = CompletableFuture
                                .runAsync(() -> {
                                    try {
                                        dao.remove(key);
                                    } catch (IOException e) {
                                        logger.error("Error while upsert local data");
                                    }
                                });
                        futures.add(future);
                    } else {
                        final CompletableFuture<Void> future =
                                client.delete(id, serviceNode.key());
                        futures.add(future);
                    }
                });
        CompletableFuture<Response> futureResp = FutureUtils.compose(futures, acks)
                .handle((values, throwable) -> handleResponses(
                        values, throwable, voids -> new Response(Response.ACCEPTED, Response.EMPTY)));
        return fromCompletableFuture(futureResp);
    }

    public Response upsert(@NotNull final String id,
                @NotNull final ByteBuffer key,
                @NotNull final byte[] body,
                final int acks,
                final int from,
                final boolean proxy) throws IOException {
        final ByteBuffer value = ByteBuffer.wrap(body);
        if(proxy) {
            dao.upsert(key, value);
            return new Response(Response.CREATED, Response.EMPTY);
        }
        final Collection<CompletableFuture<Void>> futures = new ConcurrentLinkedQueue<>();
        topology.replicas(from, key)
                .forEach(serviceNode -> {
                    if(topology.isMe(serviceNode)) {
                        CompletableFuture<Void> future = CompletableFuture
                                .runAsync(() -> {
                                    try {
                                        dao.upsert(key, value);
                                    } catch (IOException e) {
                                        logger.error("Error while upsert local data");
                                    }
                                });
                        futures.add(future);
                    } else {
                        final CompletableFuture<Void> future =
                                client.upsert(body, id,serviceNode.key());
                        futures.add(future);
                    }
                });
        CompletableFuture<Response> futureResp = FutureUtils.compose(futures, acks)
                .handle((values, throwable) -> handleResponses(
                        values, throwable, voids -> new Response(Response.CREATED, Response.EMPTY)));
        return fromCompletableFuture(futureResp);
    }

    public Response get(
             @NotNull final String id,
             @NotNull final ByteBuffer key,
             final int acks,
             final int from,
             final boolean proxy) {
        final Iterator<Cell> cellIterator = dao.latestIterator(key);
        if (proxy) {
            final Value value = Value.valueOf(cellIterator, key);
            return ResponseUtils.from(value, true);
        }

        final Collection<CompletableFuture<Value>> futures = new ConcurrentLinkedQueue<>();
        topology.replicas(from, key)
                .forEach(serviceNode -> {
                    if(topology.isMe(serviceNode)) {
                        final Future<Value> futureValue = serviceWorkers.submit(() -> Value.valueOf(cellIterator, key));
                        final CompletableFuture<Value> future = new CompletablePromise<>(futureValue);
                        futures.add(future);
                    } else {
                        final CompletableFuture<Value> future =
                                client.get(id, serviceNode.key());
                        futures.add(future);
                    }
                });

        CompletableFuture<Response> futureResp = FutureUtils.compose(futures, acks)
                .handle((values, throwable) -> handleResponses(values, throwable,
                        ResponseUtils::responseFromValues));

        return fromCompletableFuture(futureResp);
    }



    public Iterator<Record> range(@NotNull final ByteBuffer from,
                           @Nullable final ByteBuffer to) throws IOException {
        return dao.range(from, to);
    }

    @NotNull
    private <T> Response handleResponses(
            @Nullable final Collection<T> values,
            @Nullable final Throwable throwable,
            @NotNull final Function<Collection<T>, Response> response) {
        if (values != null && throwable == null) {
            return response.apply(values);
        }
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    private static Response fromCompletableFuture(@NotNull final CompletableFuture<Response> futureResponse) {
        Response response;
        try {
            response = futureResponse.get(200, TimeUnit.SECONDS);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            logger.error("Error while get response, ", e);
            response = new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
        return response;
    }
}
