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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

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

       final AtomicInteger asks = new AtomicInteger();

       final List<ServiceNode> nodes = topology.replicas(from, key);

       for (final ServiceNode node : nodes) {
            if(topology.isMe(node)) {
                CompletableFuture.runAsync(() -> {
                    try {
                        dao.remove(key);
                        asks.incrementAndGet();
                    } catch (IOException e) {
                        logger.error("Error while delete value from local storage : ", e);
                    }
                }).exceptionally(throwable -> {
                    logger.error("Error while delete value from local storage : ", throwable);
                    return null;
                });
            } else {
               try {
                   clientPool.get(node.key())
                           .delete(id)
                           .get(100, TimeUnit.MILLISECONDS);
                   asks.incrementAndGet();
               } catch (InterruptedException | ExecutionException | TimeoutException e){
                   logger.error("Can't wait response from node in url {} ", node.key(), e);
               }
            }
       }
       if(asks.get() >= acks) {
            ResponseUtils.sendResponse(session, new Response(Response.ACCEPTED, Response.EMPTY));
       } else {
           ResponseUtils.sendResponse(session, new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
       }
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
        final AtomicInteger asks = new AtomicInteger();

        final List<ServiceNode> nodes = topology.replicas(from, key);

        for (final ServiceNode node : nodes) {
            if(topology.isMe(node)) {
                CompletableFuture.runAsync(() -> {
                    try {
                        dao.upsert(key, value);
                        asks.incrementAndGet();
                    } catch (IOException e) {
                        logger.error("Error while delete value from local storage : ", e);
                    }
                }).exceptionally(throwable -> {
                    logger.error("Error while delete value from local storage : ", throwable);
                    return null;
                });
            } else {
                try {
                    clientPool.get(node.key())
                            .upsert(body,id)
                            .get(100, TimeUnit.MILLISECONDS);
                    asks.incrementAndGet();
                } catch (InterruptedException | ExecutionException | TimeoutException e){
                    logger.error("Can't wait response from node in url {} ", node.key(), e);
                }
            }
        }
        if(asks.get() >= acks) {
            ResponseUtils.sendResponse(session, new Response(Response.CREATED, Response.EMPTY));
        } else {
            ResponseUtils.sendResponse(session, new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
        }
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

        final List<ServiceNode> nodes = topology.replicas(from, key);
        final Collection<Value> values = new ArrayList<>();
        final AtomicInteger asks = new AtomicInteger(0);

        for (final ServiceNode node : nodes) {
            if(topology.isMe(node)) {
                CompletableFuture.runAsync(
                        () -> {
                            final Iterator<Cell> cellIterator = dao.latestIterator(key);
                            final Value value =
                                    Value.valueOf(cellIterator, key);
                            values.add(value);
                            asks.incrementAndGet();
                        }
                ).exceptionally(throwable -> {
                    logger.error("Error while send local response = ", throwable);
                    return null;
                });
            } else {
               try {
                   final HttpResponse<byte[]> response = clientPool.get(node.key())
                           .get(id)
                           .get(100, TimeUnit.MILLISECONDS);
                   final Value value = Value.fromHttpResponse(response);
                   values.add(value);
                   asks.incrementAndGet();
               } catch (InterruptedException | ExecutionException | TimeoutException e ){
                   logger.error("Can't wait response from node in url {} ", node.key(), e);
               }
            }
        }
        if(asks.get() >= acks) {
            final Value value = Value.merge(values);
            final Response response = ResponseUtils.from(value, false);
            ResponseUtils.sendResponse(session, response);
        } else {
            ResponseUtils.sendResponse(session, new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
        }
    }

    public Iterator<Record> range(@NotNull final ByteBuffer from,
                           @Nullable final ByteBuffer to) throws IOException {
        return dao.range(from, to);
    }
}
