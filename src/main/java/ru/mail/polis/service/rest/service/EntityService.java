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
import ru.mail.polis.client.AsyncHttpClientImpl;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.storage.cell.Cell;
import ru.mail.polis.dao.storage.cell.Value;
import ru.mail.polis.service.topology.Topology;
import ru.mail.polis.service.topology.node.ServiceNode;
import ru.mail.polis.utils.FutureUtils;
import ru.mail.polis.utils.ResponseUtils;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static ru.mail.polis.utils.ResponseUtils.sendResponse;

public final class EntityService {

    private static final Logger logger = LoggerFactory.getLogger(EntityService.class);

    private final DAO dao;
    private final Topology<ServiceNode> topology;
    private final Map<String, AsyncHttpClient> clientPool;

    public EntityService(@NotNull final DAO dao,
                  @NotNull final Topology<ServiceNode> topology) {
        this.dao = dao;
        final Executor workers = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1,
                new ThreadFactoryBuilder().setNameFormat("service-worker-%d").build());
        this.topology = topology;
        this.clientPool = new HashMap<>();
        for (final ServiceNode node : topology.all()) {
            if (!topology.isMe(node)) {
                final String url = node.key();
                assert !clientPool.containsKey(node.key());
                clientPool.put(url, new AsyncHttpClientImpl(node.key(), workers));
            }
        }
    }

    public void delete(@NotNull final HttpSession session,
                @NotNull final String id,
                @NotNull final ByteBuffer key,
                final int acks,
                final int from,
                final boolean proxy) {
        if(proxy) {
            CompletableFuture.runAsync(() -> {
                try {
                    dao.remove(key);
                    sendResponse(session, new Response(Response.ACCEPTED, Response.EMPTY));
                } catch (IOException e) {
                    logger.error("Error while delete local storage, ", e);
                    sendResponse(session, new Response(Response.INTERNAL_ERROR, Response.EMPTY));
                }
            }).exceptionally(
                    throwable -> {
                        logger.error("Error while delete to storage, ", throwable);
                        return null;
                    }
            );
            return;
        }

        final Collection<CompletableFuture<HttpResponse<byte[]>>> futures = new ArrayList<>();
        final AtomicInteger asks = new AtomicInteger(0);
        topology.replicas(from, key)
                .forEach(node -> {
                    if(topology.isMe(node)) {
                        CompletableFuture.runAsync(() -> {
                            try {
                                dao.remove(key);
                                asks.getAndIncrement();
                            } catch (IOException e) {
                                logger.error("Error upsert local value ", e);
                            }
                        }).exceptionally(throwable -> {
                            logger.error("Fail while upsert value");
                            return null;});
                    } else {
                        final AsyncHttpClient asyncHttpClient = clientPool.get(node.key());
                        final CompletableFuture<HttpResponse<byte[]>> future = asyncHttpClient.delete(id);
                        futures.add(future);
                    }
                });
        FutureUtils.getHttpResponses(futures, acks)
                .whenComplete((httpResponses, throwable) -> checkDeleteResponses(
                        asks.get(), acks, httpResponses, session))
                .exceptionally(throwable -> {
                    logger.error("Error while delete responses ", throwable);
                    return null;
                });
    }

    public void upsert(@NotNull final HttpSession session,
                @NotNull final String id,
                @NotNull final ByteBuffer key,
                @NotNull final byte[] body,
                final int acks,
                final int from,
                final boolean proxy) {
        final ByteBuffer value = ByteBuffer.wrap(body);
        if(proxy) {
            CompletableFuture.runAsync(() -> {
                try {
                    dao.upsert(key, value);
                    sendResponse(session, new Response(Response.CREATED, Response.EMPTY));
                } catch (IOException e) {
                   sendResponse(session, new Response(Response.INTERNAL_ERROR, Response.EMPTY));
                }
            }).exceptionally(throwable -> {
                logger.error("Error while local upsert value");
                return null;
            });
            return;
        }

        final Collection<CompletableFuture<HttpResponse<byte[]>>> futures = new ArrayList<>();
        final AtomicInteger asks = new AtomicInteger(0);
        topology.replicas(from, key)
            .forEach(node -> {
                    if(topology.isMe(node)) {
                        CompletableFuture.runAsync(() -> {
                            try {
                                dao.upsert(key, value);
                                asks.getAndIncrement();
                            } catch (IOException e) {
                                logger.error("Error upsert local value ", e);
                            }
                        }).exceptionally(throwable -> {
                            logger.error("Fail while upsert value");
                            return null;});
                    } else {
                        final AsyncHttpClient asyncHttpClient = clientPool.get(node.key());
                        final CompletableFuture<HttpResponse<byte[]>> future = asyncHttpClient.upsert(body, id);
                        futures.add(future);
                    }
                }
            );
        FutureUtils.getHttpResponses(futures, acks)
                .whenComplete((httpResponses, throwable) -> checkPutResponses(
                        asks.get(), acks, httpResponses, session))
                .exceptionally(throwable -> {
                    logger.error("Error while put responses ", throwable);
                    return null;
                });
    }

    public void get(@NotNull final HttpSession session,
             @NotNull final String id,
             @NotNull final ByteBuffer key,
             final int acks,
             final int from,
             final boolean proxy) {
        if (proxy) {
            CompletableFuture.runAsync(
                    () -> {
                        final Iterator<Cell> cellIterator = dao.latestIterator(key);
                        final Response response = ResponseUtils.from(
                                Value.valueOf(cellIterator, key), true
                        );
                        sendResponse(session, response);
                    }
            ).exceptionally(throwable -> {
                logger.error("Error while send local response = ", throwable);
                return null;
            });
            return;
        }

        final Collection<Value> values = new ArrayList<>();
        final List<ServiceNode> serviceNodes = topology.replicas(from, key);
        final Collection<CompletableFuture<HttpResponse<byte[]>>> futures = new ArrayList<>();

        AtomicInteger asks = new AtomicInteger(0);
        serviceNodes.forEach(node -> {
            if(topology.isMe(node)) {
                CompletableFuture.runAsync(() -> {
                    final Iterator<Cell> cellIterator = dao.latestIterator(key);
                    final Value value = Value.valueOf(cellIterator, key);
                    values.add(value);
                    asks.getAndIncrement();
                }).exceptionally(throwable -> {
                    logger.error("Fail while upsert value");
                    return null;});
            } else {
                final AsyncHttpClient client = clientPool.get(node.key());
                futures.add(client.get(id));
            }});

        FutureUtils.getHttpResponses(futures, acks)
            .whenComplete(
                    (httpResponses, throwable) -> checkGetResponses(
                            asks.get(), acks, values, httpResponses, session)
            ).exceptionally(
                    throwable -> {
                        logger.error("Error while get responses ", throwable);
                        return null;
                    });
    }

    private static void checkDeleteResponses(final int startAcks,
                                            final int expectedAcks,
                                            @NotNull final Collection<HttpResponse<byte[]>> responses,
                                            @NotNull final HttpSession session) {
        final int acks = countAcks(
                startAcks,
                responses, r -> r.statusCode() == 202);
        checkAcks(acks, expectedAcks,
                () -> new Response(Response.ACCEPTED, Response.EMPTY),
                session);
    }

    private static void checkGetResponses(final int startAcks,
                                         final int expectedAcks,
                                         @NotNull final Collection<Value> values,
                                         @NotNull final Collection<HttpResponse<byte[]>> responses,
                                         @NotNull final HttpSession session) {
        final int acks = countAcks(
                startAcks,
                responses, r -> values.add(Value.fromHttpResponse(r)));
        checkAcks(acks, expectedAcks, () -> ResponseUtils.from(Value.merge(values), false),
                session);
    }

    private static void checkPutResponses(final int startAcks,
                                          final int expectedAcks,
                                          @NotNull final Collection<HttpResponse<byte[]>> responses,
                                          @NotNull final HttpSession session) {
        final int acks = countAcks(
                startAcks,
                responses, r -> r.statusCode() == 201);
        checkAcks(acks, expectedAcks,
                () -> new Response(Response.CREATED, Response.EMPTY),
                session);
    }

    private static int countAcks(final int startAcks,
                                 @NotNull final Collection<HttpResponse<byte[]>> responses,
                                 @NotNull final Predicate<HttpResponse<byte[]>> successful) {
        int acks = startAcks;
        for (final HttpResponse<byte[]> response : responses) {
            if (successful.test(response)) {
                acks++;
            }
        }
        return acks;
    }

    private static void checkAcks(final int acks,
                                  final int expectedAcks,
                                  @NotNull final Supplier<Response> supplier,
                                  @NotNull final HttpSession session) {
        if (acks >= expectedAcks) {
            sendResponse(session, supplier.get());
        } else {
            sendResponse(session, new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
        }
    }

    public Iterator<Record> range(@NotNull final ByteBuffer from,
                           @Nullable final ByteBuffer to) throws IOException {
        return dao.range(from, to);
    }
}
