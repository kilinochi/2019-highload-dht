package ru.mail.polis.service.rest;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.HttpServerConfig;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import ru.mail.polis.Record;
import ru.mail.polis.client.AsyncHttpClient;
import ru.mail.polis.client.AsyncHttpClientImpl;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.storage.cell.Cell;
import ru.mail.polis.dao.storage.cell.Value;
import ru.mail.polis.utils.BytesUtils;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.rest.session.StorageSession;
import ru.mail.polis.service.topology.Topology;
import ru.mail.polis.service.topology.node.ServiceNode;
import ru.mail.polis.utils.ResponseUtils;

import static ru.mail.polis.utils.ResponseUtils.build;
import static ru.mail.polis.utils.ResponseUtils.sendResponse;

public final class RestController extends HttpServer implements Service {
    public static final String TIMESTAMP_HEADER = "X-OK-Timestamp";
    private static final String PROXY_HEADER = "X-OK-Proxy: True";

    private static final Logger logger = LoggerFactory.getLogger(RestController.class);

    private final RF defaultRF;
    private final int nodesSize;
    private final DAO dao;
    private final Topology<ServiceNode> topology;
    private final Map<String, AsyncHttpClient> clientPool;

    /**
     * Create new instance of RestService for interaction with database.
     *
     * @param config in config for server
     * @param dao    is dao for interaction with database
     * @param nodes  all nodes in cluster
     * @param me     is current node
     */
    private RestController(
            @NotNull final HttpServerConfig config,
            @NotNull final DAO dao,
            @NotNull final Topology<ServiceNode> nodes,
            @NotNull final ServiceNode me) throws IOException {
        super(config);
        this.nodesSize = nodes.size();
        this.clientPool = new HashMap<>();
        this.defaultRF = new RF(nodes.size() / 2 + 1, nodes.size());
        for (final ServiceNode node : nodes.all()) {
            if (!node.equals(me)) {
                final String url = node.key();
                assert !clientPool.containsKey(node.key());
                clientPool.put(url, new AsyncHttpClientImpl(node.key()));
            }
        }
        this.dao = dao;
        this.topology  = nodes;
    }

    /**
     * Build new instance of RestService.
     *
     * @param port  is port on witch Service will be running
     * @param dao   is dao for interaction with database
     * @param nodes is all nodes in the cluster
     */
    public static RestController create(
            final int port,
            @NotNull final DAO dao,
            @NotNull final Topology<ServiceNode> nodes) throws IOException {
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        final ServiceNode me = nodes.all()
                .stream()
                .filter(serviceNode -> serviceNode.key()
                        .endsWith(String.valueOf(port)))
                .findFirst().get();
        final HttpServerConfig httpServerConfig = new HttpServerConfig();
        httpServerConfig.acceptors = new AcceptorConfig[]{acceptorConfig};
        httpServerConfig.minWorkers = Runtime.getRuntime().availableProcessors() + 1;
        httpServerConfig.maxWorkers = Runtime.getRuntime().availableProcessors() + 1;
        return new RestController(httpServerConfig, dao, nodes, me);
    }

    @Override
    public void handleDefault(
            @NotNull final Request request,
            @NotNull final HttpSession session) {
        sendResponse(session, build(Response.BAD_REQUEST, Response.EMPTY));
    }

    @Override
    public HttpSession createSession(@NotNull final Socket socket) {
        return new StorageSession(socket, this);
    }

    /**
     * Rest-endpoint for this uri.
     */
    @Path("/v0/status")
    public Response status() {
        return build(Response.OK, Response.EMPTY);
    }

    /**
     * Rest-endpoint with this uri.
     *
     * @param start   is parameters for uri
     * @param end     is parameters for uri
     * @param request is request on this uri
     * @param session is current session
     */
    @Path("/v0/entities")
    public void entities(
            @Param("start") final String start,
            @Param("end") final String end,
            @NotNull final Request request,
            @NotNull final HttpSession session) {
        logger.info("Start with : {} and end with : {} ", start, end);
        if (start == null || start.isEmpty()) {
            sendResponse(session, build(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        if (end != null && end.isEmpty()) {
            sendResponse(session, build(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        if (request.getMethod() != Request.METHOD_GET) {
            sendResponse(session, build(Response.METHOD_NOT_ALLOWED, Response.EMPTY));
            return;
        }
        try {
            final Iterator<Record> recordIterator = dao.range(BytesUtils.keyByteBuffer(start),
                    end == null ? null : BytesUtils.keyByteBuffer(end));
            ((StorageSession) session).stream(recordIterator);
        } catch (IOException e) {
            logger.error("Something wrong while get range of value ", e.getCause());
        }
    }

    /**
     * Rest-endpoint with this uri.
     *
     * @param id       is parameters for uri
     * @param replicas is replication factor in this endpoint
     * @param request  is request on this uri
     * @param session  is current session
     */
    @Path("/v0/entity")
    public void entity(
            @Param("id") final String id,
            @Param("replicas") final String replicas,
            final Request request,
            final HttpSession session) {
        if (id == null || id.isEmpty()) {
            sendResponse(session, build(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }

        boolean proxied = false;
        if (request.getHeader(PROXY_HEADER) != null) {
            proxied = true;
        }

        final RF rf;
        try {
            rf = replicas == null ? defaultRF : RF.of(replicas);
            if (rf.ack < 1 || rf.from < rf.ack || rf.from > nodesSize) {
                throw new IllegalArgumentException("From is too big!");
            }
        } catch (IllegalArgumentException e) {
            sendResponse(session, build(Response.BAD_REQUEST, "WrongRF".getBytes(Charsets.UTF_8)));
            return;
        }

        final int ask = rf.ack;
        final int from = rf.from;
        final boolean finalProxied = proxied;
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                asyncExecute(session, () -> get(id, ask, from, finalProxied));
                break;
            case Request.METHOD_PUT:
                asyncExecute(session, () -> upsert(id, request.getBody(), ask, from, finalProxied));
                break;
            case Request.METHOD_DELETE:
                asyncExecute(session, () -> delete(id, ask, from, finalProxied));
                break;
            default:
                logger.warn("Not supported HTTP-method: {}", request.getMethod());
                sendResponse(session, build(Response.METHOD_NOT_ALLOWED, Response.EMPTY));
                break;
        }
    }

    private void asyncExecute(
            @NotNull final HttpSession session,
            @NotNull final ResponsePublisher publisher) {
        asyncExecute(() -> {
            try {
                sendResponse(session, publisher.submit());
            } catch (IOException e) {
                logger.error("Unable to create response ", e.getCause());
                try {
                    session.sendError(Response.INTERNAL_ERROR, "Error while send response");
                } catch (IOException ioException) {
                    logger.error("Error while send response ", ioException.getCause());
                }
            }
        });
    }

    private Response get(@NotNull final String id,
                         final int acks,
                         final int from,
                         final boolean proxy) {
        final ByteBuffer key = BytesUtils.keyByteBuffer(id);
        final Iterator<Cell> cellIterator = dao.latestIterator(key);
        if(proxy) {
            final Value value = Value.valueOf(cellIterator, key);
            return ResponseUtils.from(value, proxy);
        }

        final Collection<Value> valuesFromResponses = new ArrayList<>();

        final ServiceNode[] nodes = topology.replicas(from, key);
        int asks = 0;
        for(ServiceNode node : nodes) {
            if(topology.isMe(node)) {
                valuesFromResponses.add(Value.valueOf(cellIterator, key));
                asks++;
            } else {
                try {
                    CompletableFuture<HttpResponse<byte[]>> future =
                            clientPool.get(node.key())
                                    .get(id);
                    HttpResponse<byte[]> response = future.get(2, TimeUnit.SECONDS);
                    Value value = Value.fromHttpResponse(response);
                    valuesFromResponses.add(value);
                    asks++;
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    logger.error("Can't wait in get method ", e);
                }
            }
        }
        if(asks >= acks) {
            Value mergeValue = Value.merge(valuesFromResponses);
            return ResponseUtils.from(mergeValue, proxy);
        }
        else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response delete(@NotNull final String id,
                            final int acks,
                            final int from,
                            final boolean proxy) throws IOException {
        final ByteBuffer key = BytesUtils.keyByteBuffer(id);
        if(proxy) {
            dao.remove(key);
            return new Response(Response.ACCEPTED, Response.EMPTY);
        }

        final ServiceNode[] nodes = topology.replicas(from, key);
        int asks = 0;
        for (ServiceNode node : nodes) {
            if(topology.isMe(node)) {
                dao.remove(key);
                asks++;
            } else {
                try {
                    clientPool
                            .get(node.key())
                            .delete(id)
                            .get(2, TimeUnit.SECONDS);
                    asks++;
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    logger.error("Cant get response in delete method ", e);
                }
            }
        }
        if(asks >= acks) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response upsert(@NotNull final String id,
                            final byte[] body,
                            final int acks,
                            final int from,
                            final boolean proxy) throws IOException {
        final ByteBuffer key = BytesUtils.keyByteBuffer(id);
        final ByteBuffer value = ByteBuffer.wrap(body);
        if(proxy) {
            dao.upsert(key, value);
            return new Response(Response.OK, Response.EMPTY);
        }

        final ServiceNode[] nodes = topology.replicas(from, key);
        int asks = 0;
        for(ServiceNode node : nodes) {
            if(topology.isMe(node)) {
                dao.upsert(key, value);
                asks++;
            } else {
                try {
                    clientPool.get(node.key())
                            .upsert(body, id)
                            .get(2, TimeUnit.SECONDS);
                    asks++;
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    logger.error("Cant get response in upsert method, ", e);
                }
            }
        }
        if(asks >= acks) {
            return new Response(Response.CREATED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    @FunctionalInterface
    private interface ResponsePublisher {
        Response submit() throws IOException;
    }

    private static final class RF {
        private final int ack;
        private final int from;

        private RF(final int ack, final int from) {
            this.ack = ack;
            this.from = from;
        }

        @NotNull
        private static RF of(@NotNull final String value) {
            final List<String> values = Splitter.on('/').splitToList(value);
            if (values.size() != 2) {
                throw new IllegalArgumentException("Wrong replica factor:" + value);
            }
            return new RF(Integer.parseInt(values.get(0)), Integer.parseInt(values.get(1)));
        }
    }
}
