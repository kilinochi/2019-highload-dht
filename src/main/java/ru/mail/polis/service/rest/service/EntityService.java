package ru.mail.polis.service.rest.service;

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
import ru.mail.polis.service.topology.Topology;
import ru.mail.polis.service.topology.node.ServiceNode;
import ru.mail.polis.utils.ResponseUtils;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public final class EntityService {

    private static final Logger logger = LoggerFactory.getLogger(EntityService.class);

    private final DAO dao;
    private final Topology<ServiceNode> topology;
    private final AsyncHttpClient client;

    public EntityService(@NotNull final DAO dao,
                  @NotNull final Topology<ServiceNode> topology) {
        this.dao = dao;
        this.topology = topology;
        this.client = AsyncHttpClient.create();
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
       int asks = 0;

       final List<ServiceNode> nodes = topology.replicas(from, key);

       for (final ServiceNode node : nodes) {
            if(topology.isMe(node)) {
                dao.remove(key);
                asks++;
            } else {
               try {
                   client.delete(id, node.key())
                           .get();
                   asks++;
               } catch (InterruptedException | ExecutionException e){
                   logger.error("Can't wait response from node in url {} ", node.key(), e);
               }
            }
       }
       if(asks >= acks) {
           return new Response(Response.ACCEPTED, Response.EMPTY);
       } else {
           return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
       }
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
        int asks = 0;

        final List<ServiceNode> nodes = topology.replicas(from, key);

        for (final ServiceNode node : nodes) {
            if(topology.isMe(node)) {
                dao.upsert(key, value);
                asks++;
            } else {
                try {
                    client
                            .upsert(body,id, node.key())
                            .get();
                    asks++;
                } catch (InterruptedException | ExecutionException e){
                    logger.error("Can't wait response from node in url {} ", node.key(), e);
                }
            }
        }
        if(asks >= acks) {
            return new Response(Response.CREATED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
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
            return ResponseUtils.from(value, proxy);
        }

        final List<ServiceNode> nodes = topology.replicas(from, key);
        final Collection<Value> values = new ArrayList<>();
        int asks = 0;

        for (final ServiceNode node : nodes) {
            if(topology.isMe(node)) {
                final Value value = Value.valueOf(cellIterator, key);
                values.add(value);
                asks++;
            } else {
               try {
                   final HttpResponse<byte[]> response = client
                           .get(id, node.key())
                           .get();
                   final Value value = Value.fromHttpResponse(response);
                   values.add(value);
                   asks++;
               } catch (InterruptedException | ExecutionException e){
                   logger.error("Can't wait response from node in url {} ", node.key(), e);
               }
            }
        }
        if(asks >= acks) {
            final Value value = Value.merge(values);
            return ResponseUtils.from(value, false);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    public Iterator<Record> range(@NotNull final ByteBuffer from,
                           @Nullable final ByteBuffer to) throws IOException {
        return dao.range(from, to);
    }
}
