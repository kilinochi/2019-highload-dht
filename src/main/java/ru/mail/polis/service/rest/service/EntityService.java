package ru.mail.polis.service.rest.service;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.client.AsyncHttpClient;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.replica.RF;
import ru.mail.polis.dao.storage.cell.Value;
import ru.mail.polis.service.topology.Topology;
import ru.mail.polis.service.topology.node.ServiceNode;
import ru.mail.polis.utils.BytesUtils;
import ru.mail.polis.utils.FutureUtils;
import ru.mail.polis.utils.ResponseUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static ru.mail.polis.utils.ResponseUtils.sendResponse;

public final class EntityService {

    private static final Logger logger = LoggerFactory.getLogger(EntityService.class);

    private final DAO dao;
    private final Topology<ServiceNode> topology;
    private final AsyncHttpClient client;
    private final ExecutorService serviceWorkers;

    /**
     * Service for interaction to dao.
     *
     * @param dao      is storage
     * @param topology is node topology
     */
    public EntityService(@NotNull final DAO dao,
                         @NotNull final Topology<ServiceNode> topology) {
        this.dao = dao;
        this.topology = topology;
        this.client = AsyncHttpClient.create();
        serviceWorkers = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors() + 2,
                new ThreadFactoryBuilder().setNameFormat("entity-service-workers-%d").build());
    }

    /**
     * Delete value from dao by id.
     *
     * @param id    is id
     * @param rf    is replica factor
     * @param proxy is proxy or not current node
     */
    public void delete(
            @NotNull final String id,
            @NotNull final RF rf,
            @NotNull final HttpSession session,
            final boolean proxy) {
        final ByteBuffer key = BytesUtils.keyByteBuffer(id);
        if (proxy) {
            handleLocal(() -> {
                deleteLocalValue(key);
                sendResponse(session, new Response(Response.ACCEPTED, Response.EMPTY));
            }).exceptionally(throwable -> {
                exceptionallyHandle(session, throwable);
                return null;
            });
            return;
        }
        final int from = rf.getFrom();
        final int acks = rf.getAck();
        final Collection<CompletableFuture<Void>> futures = new ConcurrentLinkedQueue<>();
        topology.replicas(from, key)
                .forEach(serviceNode -> {
                    final CompletableFuture<Void> future;
                    if (topology.isMe(serviceNode)) {
                        future = handleLocal(() -> deleteLocalValue(key));
                    } else {
                        future = client.delete(id, serviceNode.key());
                    }
                    futures.add(future);
                });

        responseFuture(futures, HttpMethods.DELETE, acks)
                .whenCompleteAsync((response, throwable) -> sendResponse(session, response))
                .exceptionally(throwable -> {
                    exceptionallyHandle(session, throwable);
                    return null;
                });
    }

    /**
     * Upsert value in dao by id.
     *
     * @param id    is id
     * @param body  in value to upsert
     * @param rf    is replica factor
     * @param proxy is proxy or not current node
     */
    public void upsert(@NotNull final String id,
                       @NotNull final RF rf,
                       @NotNull final HttpSession session,
                       @NotNull final byte[] body,
                       final boolean proxy) {
        final ByteBuffer value = ByteBuffer.wrap(body);
        final ByteBuffer key = BytesUtils.keyByteBuffer(id);
        if (proxy) {
            handleLocal(() -> {
                upsertLocalValue(key, value);
                sendResponse(session, new Response(Response.CREATED, Response.EMPTY));
            }).exceptionally(throwable -> {
                exceptionallyHandle(session, throwable);
                return null;
            });
            return;
        }
        final int from = rf.getFrom();
        final int acks = rf.getAck();
        final Collection<CompletableFuture<Void>> futures = new ConcurrentLinkedQueue<>();
        topology.replicas(from, key)
                .forEach(serviceNode -> {
                    final CompletableFuture<Void> future;
                    if (topology.isMe(serviceNode)) {
                        future = handleLocal(() -> upsertLocalValue(key, value));
                    } else {
                        future = client.upsert(body, id, serviceNode.key());

                    }
                    futures.add(future);
                });

        responseFuture(futures, HttpMethods.PUT, acks)
                .whenCompleteAsync((response, throwable) -> sendResponse(session, response))
                .exceptionally(throwable -> {
                    exceptionallyHandle(session, throwable);
                    return null;
                });
    }

    /**
     * Get value in dao by id.
     *
     * @param id    is id
     * @param rf    is replica factor
     * @param proxy is proxy or not current node
     */
    public void get(
            @NotNull final String id,
            @NotNull final RF rf,
            @NotNull final HttpSession session,
            final boolean proxy) {
        final ByteBuffer key = BytesUtils.keyByteBuffer(id);
        if (proxy) {
            handleLocal(() -> {
                final Value value = getLocalValue(key);
                final Response response = ResponseUtils.from(value, true);
                sendResponse(session, response);
            }).exceptionally(throwable -> {
                exceptionallyHandle(session, throwable);
                return null;
            });
            return;
        }
        final int from = rf.getFrom();
        final int acks = rf.getAck();
        final Collection<CompletableFuture<Value>> futures = new ConcurrentLinkedQueue<>();
        topology.replicas(from, key)
                .forEach(serviceNode -> {
                    final CompletableFuture<Value> future;
                    if (topology.isMe(serviceNode)) {
                        future = getLocal(key);
                    } else {
                        future = client.get(id, serviceNode.key());
                    }
                    futures.add(future);
                });

        FutureUtils.collapseFutures(futures, acks)
                .handleAsync((values, throwable) -> {
                    if (throwable == null && values != null) {
                        return ResponseUtils.responseFromValues(values);
                    }
                    return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
                }).whenCompleteAsync((response, throwable) -> sendResponse(session, response))
                .exceptionally(throwable -> {
                    exceptionallyHandle(session, throwable);
                    return null;
                });
    }

    public Iterator<Record> range(@NotNull final ByteBuffer from,
                                  @Nullable final ByteBuffer to) throws IOException {
        return dao.range(from, to);
    }

    private static <T> CompletableFuture<Response> responseFuture(
            @NotNull final Collection<CompletableFuture<T>> futures,
            @NotNull final HttpMethods httpMethods,
            final int acks) {
        return FutureUtils.collapseFutures(futures, acks)
                .handleAsync((values, throwable) -> createResponse(throwable, values, httpMethods));
    }

    private static <T> Response createResponse(@Nullable final Throwable throwable,
                                               @Nullable final Collection<T> values,
                                               @NotNull final HttpMethods method) {
        if (throwable == null && values != null) {
            switch (method) {
                case PUT:
                    return new Response(Response.CREATED, Response.EMPTY);
                case DELETE:
                    return new Response(Response.ACCEPTED, Response.EMPTY);
                default:
                    return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
            }
        }
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    private CompletableFuture<Void> handleLocal(@NotNull final Runnable task) {
        return CompletableFuture.runAsync(task, serviceWorkers);
    }

    private CompletableFuture<Value> getLocal(@NotNull final ByteBuffer key) {
        return CompletableFuture.supplyAsync(() -> getLocalValue(key), serviceWorkers);
    }

    private Value getLocalValue(@NotNull final ByteBuffer key) {
        return Value.fromIterator(key, dao.latestIterator(key));
    }

    private void upsertLocalValue(@NotNull final ByteBuffer key,
                                  @NotNull final ByteBuffer value) {
        try {
            dao.upsert(key, value);
        } catch (IOException e) {
            logger.error("Error while upsert local data : ", e);
        }
    }

    private void deleteLocalValue(@NotNull final ByteBuffer key) {
        try {
            dao.remove(key);
        } catch (IOException e) {
            logger.error("Error while delete local data : ", e);
        }
    }

    private static void exceptionallyHandle(@NotNull final HttpSession session,
                                            @NotNull final Throwable throwable) {
        logger.error("Failed CRUD operation in local storage", throwable);
        try {
            session.sendError(Response.INTERNAL_ERROR, "Error while send response");
        } catch (IOException ioException) {
            logger.error("Error while send error ", ioException.getCause());
        }
    }

    private enum HttpMethods {
        PUT, DELETE
    }
}
