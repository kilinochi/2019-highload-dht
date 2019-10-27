package ru.mail.polis.service.rest;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.Response;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;

import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.storage.cell.Cell;
import ru.mail.polis.dao.storage.cell.CellValue;
import ru.mail.polis.service.topology.Topology;
import ru.mail.polis.service.topology.node.ServiceNode;
import ru.mail.polis.utils.BytesUtils;
import ru.mail.polis.utils.CellUtils;
import ru.mail.polis.utils.ResponseUtils;

import static ru.mail.polis.service.rest.RestController.PROXY_HEADER;


final class DaoService {

    private static final Logger logger = LoggerFactory.getLogger(DaoService.class);

    private final DAO dao;
    private final Map<String, HttpClient> clientPool;
    private final Topology<ServiceNode> nodes;
    private final ServiceNode me;

    /**
     * Create service for interaction to dao.
     * @param nodes is nodes to the cluster
     * @param me is current node
     * @param dao is dao
     * @param clientPool is pool of client
     */
    DaoService(@NotNull final DAO dao,
               @NotNull final Map<String, HttpClient> clientPool,
               @NotNull final Topology<ServiceNode> nodes,
               @NotNull final ServiceNode me) {
        this.dao = dao;
        this.clientPool = clientPool;
        this.nodes = nodes;
        this.me = me;
    }

    Response delete(@NotNull final String id,
                    final int ask,
                    final int from,
                    final boolean proxy) throws IOException {
        final ByteBuffer key = BytesUtils.keyByteBuffer(id);
        if (proxy) {
            dao.remove(key);
            return new Response(Response.ACCEPTED, Response.EMPTY);
        }

        final ServiceNode[] serviceNodes = nodes.replicas(from, key);
        int asks = 0;
        for (final ServiceNode serviceNode : serviceNodes) {
            if (!serviceNode.equals(me)) {
                try {
                    final Response response = clientPool.get(serviceNode.key())
                            .delete("/v0/entity?id=" + id, PROXY_HEADER);
                    logger.info("We proxy our request to another node : {}", serviceNode.key());
                    if (response.getStatus() == 202) {
                        logger.info("OK, status is {}", response.getStatus());
                        asks++;
                    }
                } catch (InterruptedException | PoolException | HttpException e) {
                    logger.info("Can not wait answer from client {} in host {}" , e.getMessage(), serviceNode.key());
                }
            } else {
                logger.info("Our host is {} and we remove value", me.key());
                dao.remove(key);
                asks++;
            }
        }
        if(asks >= ask) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    Response get(@NotNull final String id,
                 final int ask,
                 final int from,
                 final boolean proxy) throws IOException {
        final ByteBuffer key = BytesUtils.keyByteBuffer(id);
        final Iterator<Cell> cellsIt = dao.cellIterator(key);
        final CellValue cells = CellUtils.value(key, cellsIt);
        if (proxy) {
            return ResponseUtils.from(cells, true);
        }

        final ServiceNode[] nodes = this.nodes.replicas(from, key);
        final List<CellValue> responses = new ArrayList<>();

        int asks = 0;
        for (final ServiceNode node : nodes) {
            if (!node.equals(me)) {
                try {
                    final Response response = clientPool.get(node.key())
                            .get("/v0/entity?id=" + id, PROXY_HEADER);
                    logger.info("We proxy our request to another node : {}", node.key());
                    asks++;
                    responses.add(CellUtils.getFromResponse(response));
                } catch (InterruptedException | PoolException | HttpException e) {
                    logger.info("Can not wait answer from client {} in host {}" , e.getMessage(), node.key());
                }
            } else {
                logger.info("Our host is {} and get value", me.key());
                responses.add(cells);
                asks++;
            }
        }
        if (asks >= ask) {
            return ResponseUtils.from(CellUtils.merge(responses), false);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    Response upsert(@NotNull final String id,
                    @NotNull final byte[] value,
                    final int ask,
                    final int from,
                    final boolean proxy) throws IOException {
        final ByteBuffer key = BytesUtils.keyByteBuffer(id);
        final ByteBuffer byteBufferValue = ByteBuffer.wrap(value);
        if (proxy) {
            dao.upsert(key, byteBufferValue);
            return new Response(Response.CREATED, Response.EMPTY);
        }
        final ServiceNode[] nodes = this.nodes.replicas(from, key);
        int asks = 0;
        for (final ServiceNode node : nodes) {
            if (!node.equals(me)) {
                try {
                    final Response response = clientPool.get(node.key()).put(
                            "/v0/entity?id=" + id, value, PROXY_HEADER);
                    logger.info("We proxy our request to another node : {}", node.key());
                    if (response.getStatus() == 201) {
                        logger.info("OK, status is {}", response.getStatus());
                        asks++;
                    }
                } catch (InterruptedException | PoolException | HttpException e) {
                    logger.info("Can not wait answer from client {} in host {}" , e.getMessage(), node.key());
                }
            } else {
                logger.info("Our host is {} and we upsert value", me.key());
                dao.upsert(key, byteBufferValue);
                asks++;
            }
        }
        if (asks >= ask) {
            return new Response(Response.CREATED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    Iterator<Record> range(@NotNull final ByteBuffer from,
                                  @Nullable final ByteBuffer to) throws IOException {
        return dao.range(from, to);
    }

    public Response action(@NotNull final Request request,
                           @NotNull final String id,
                           @NotNull )
}
