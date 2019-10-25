package ru.mail.polis.service.rest;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import one.nio.http.HttpClient;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.HttpServerConfig;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.HttpException;
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.storage.cell.CellValue;
import ru.mail.polis.dao.storage.utils.BytesUtils;
import ru.mail.polis.dao.storage.utils.CellUtils;
import ru.mail.polis.dao.storage.utils.ResponseUtils;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.rest.session.StorageSession;
import ru.mail.polis.service.topology.Topology;
import ru.mail.polis.service.topology.node.ServiceNode;

public final class RestService extends HttpServer implements Service {
    public static final String TIMESTAMP_HEADER = "X-OK-Timestamp";

    private static final Logger logger = LoggerFactory.getLogger(RestService.class);
    private static final String PROXY_HEADER = "X-OK-Proxy: True";

    private final RF defaultRF;
    private final Topology<ServiceNode> nodes;
    private final DAO dao;
    private final Map<String, HttpClient> pool;
    private final ServiceNode me;

    /**
     * Create new instance of RestService for interaction with database.
     * @param config in config for server
     * @param dao is dao for interaction with database
     * @param nodes all nodes in cluster
     * @param me is current node
     */
    private RestService(
            @NotNull final HttpServerConfig config,
            @NotNull final DAO dao,
            @NotNull final Topology<ServiceNode> nodes,
            @NotNull final ServiceNode me) throws IOException {
        super(config);
        this.dao = dao;
        this.me = me;
        this.nodes = nodes;
        this.pool = new HashMap<>();
        this.defaultRF = new RF(nodes.size() / 2 + 1 , nodes.size());
        for(final ServiceNode node : this.nodes.all()) {
            if(!node.equals(this.me)) {
                final String url = node.key();
                assert !pool.containsKey(node.key());
                pool.put(url, new HttpClient(new ConnectionString(url + "?timeout=100")));
            }
        }
    }

    /**
     * Build new instance of RestService.
     * @param port is port on witch Service will be running
     * @param dao is dao for interaction with database
     * @param nodes is all nodes in the cluster
     */
    public static RestService create(
            final int port,
            @NotNull final DAO dao,
            @NotNull final Topology<ServiceNode> nodes) throws IOException {
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        final ServiceNode me = nodes.all()
                .stream()
                .filter(serviceNode -> serviceNode.key()
                .endsWith(""+port))
                .findFirst().get();
        final HttpServerConfig httpServerConfig = new HttpServerConfig();
        httpServerConfig.acceptors = new AcceptorConfig[]{acceptorConfig};
        httpServerConfig.minWorkers = Runtime.getRuntime().availableProcessors();
        httpServerConfig.maxWorkers = Runtime.getRuntime().availableProcessors();
        return new RestService(httpServerConfig, dao, nodes, me);
    }

    @Override
    public void handleDefault(
            @NotNull final Request request,
            @NotNull final HttpSession session) {
        ResponseUtils.sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
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
        return new Response(Response.OK, Response.EMPTY);
    }

    /**
     * Rest-endpoint with this uri.
     * @param start is parameters for uri
     * @param end is parameters for uri
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
            ResponseUtils.sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        if (end != null && end.isEmpty()) {
            ResponseUtils.sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        if (request.getMethod() != Request.METHOD_GET) {
            ResponseUtils.sendResponse(session, new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY));
            return;
        }
        try {
            final Iterator<Record> recordIterator = dao.range(ByteBuffer.wrap(start.getBytes(Charsets.UTF_8)),
                    end == null ? null : ByteBuffer.wrap(end.getBytes(Charsets.UTF_8)));
            ((StorageSession) session).stream(recordIterator);
        } catch (IOException e) {
            logger.error("Something wrong while get range of value {}", e.getMessage());
        }
    }

    /**
     * Rest-endpoint with this uri.
     * @param id is parameters for uri
     * @param replicas is replication factor in this endpoint
     * @param request is request on this uri
     * @param session is current session
     */
    @Path("/v0/entity")
    public void entity(
            @Param("id") final String id,
            @Param("replicas") final String replicas,
            final Request request,
            final HttpSession session) {
        if (id == null || id.isEmpty()) {
            ResponseUtils.sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        final ByteBuffer key = BytesUtils.keyByteBuffer(id);

        final boolean proxied = request.getHeader(PROXY_HEADER) != null;

        RF rf = null;
        try {
            rf = replicas == null ? defaultRF : RF.of(replicas);
            if(rf.ask < 1 || rf.from < rf.ask || rf.from > nodes.size()) {
                throw new IllegalArgumentException("From is too big!");
            }
        } catch (IllegalArgumentException e) {
            ResponseUtils.sendResponse(session, new Response(Response.BAD_REQUEST, "WrongRF".getBytes(Charsets.UTF_8)));
        }

        final ServiceNode primary = nodes.primaryFor(key);
        if(!nodes.isMe(primary)) {
            asyncExecute(session, () -> proxy(primary, request));
            return;
        }
        final RF finalRf = rf;
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                asyncExecute(session, () -> get(id, finalRf, proxied));
                break;
            case Request.METHOD_PUT:
                asyncExecute(session, () -> upsert(id, request.getBody(), finalRf, proxied));
                break;
            case Request.METHOD_DELETE:
                asyncExecute(session, () -> delete(id));
                break;
            default:
                logger.warn("Not supported HTTP-method: {}", request.getMethod());
                ResponseUtils.sendResponse(session, new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY));
                break;
        }
    }

    private void asyncExecute(
            @NotNull final HttpSession session,
            @NotNull final ResponsePublisher publisher) {
        asyncExecute(() -> {
            try {
                ResponseUtils.sendResponse(session, publisher.submit());
            } catch (IOException e) {
                logger.error("Unable to create response {} ", e.getMessage());
                try {
                    session.sendError(Response.INTERNAL_ERROR, "Error while send response");
                } catch (IOException ioExecption) {
                    logger.error("Error while send response {}", ioExecption.getMessage());
                }
            } catch (NoSuchElementException e) {
                try {
                    session.sendError(Response.NOT_FOUND, "Not found recourse!");
                } catch (IOException ex) {
                    logger.error("Error while send error {}", ex.getMessage());
                }
            }
        });
    }

    private Response proxy(
            @NotNull final ServiceNode node,
            @NotNull final Request request) throws IOException {
        assert !nodes.isMe(node);
        try {
            logger.info("We proxy our request to another node: {}", node.key());
            return pool.get(node.key()).invoke(request);
        } catch (InterruptedException | PoolException | HttpException e) {
            throw (IOException) new IOException().initCause(e);
        }
    }

    private Response upsert(
            @NotNull final String id,
            @NotNull final byte[] value,
            @NotNull final RF rf,
            final boolean isProxy) throws IOException {
        final ByteBuffer key = BytesUtils.keyByteBuffer(id);
        try {
           if (isProxy) {
               dao.upsert(key, ByteBuffer.wrap(value));
               return new Response(Response.CREATED, Response.EMPTY);
           }
           final ServiceNode[] nodes = this.nodes.replicas(rf.from, key);
           int ask = 0;
           for (final ServiceNode node : nodes) {
               if (node.equals(me)) {
                   dao.upsert(key, ByteBuffer.wrap(value));
                   ask++;
               } else {
                   final Response response = pool.get(node.key()).put(
                           "/v0/entity?id=" + id, value, PROXY_HEADER);
                   if (response.getStatus() == 201) {
                       ask++;
                   }
               }
           }
           if (ask >= rf.ask) {
               return new Response(Response.ACCEPTED, Response.EMPTY);
           } else {
               return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
           }
        } catch (InterruptedException | PoolException | HttpException e) {
            throw (IOException) new IOException().initCause(e);
        }
    }

    private Response delete(
            @NotNull final String id) throws IOException {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        dao.remove(key);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response get(
            @NotNull final String id,
            @NotNull final RF rf,
            final boolean proxy) throws IOException, NoSuchElementException {
        final ByteBuffer key = BytesUtils.keyByteBuffer(id);
        try {
            if (proxy) {
                final byte[] val = BytesUtils.getBytesFromKey(id);
                final CellValue cells = CellUtils.getCells(val, dao);
                return ResponseUtils.from(cells, proxy);
            }

            final ServiceNode[] nodes = this.nodes.replicas(rf.from, key);
            final List<CellValue> responses = new ArrayList<>();

            int ack = 0;
            for (final ServiceNode node : nodes) {
                if (node.equals(me)) {
                    final byte[] val = BytesUtils.getBytesFromKey(id);
                    final CellValue cells = CellUtils.getCells(val, dao);
                    responses.add(cells);
                    ack++;
                } else {
                    final Response response = pool.get(node.key())
                            .get("/v0/entity?id=" + id, PROXY_HEADER);
                    ack++;
                    responses.add(CellUtils.getFromResponse(response));
                }
            }
            if (ack >= rf.ask) {
                return ResponseUtils.from(CellUtils.merge(responses), proxy);
            } else {
                return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
            }
        } catch (InterruptedException | PoolException | HttpException e) {
            throw (IOException) new IOException().initCause(e);
        }
    }

    @FunctionalInterface
    private interface ResponsePublisher {
        Response submit() throws IOException;
    }

    private static final class RF {
        private final int ask;
        private final int from;

        private RF(final int ask, final int from) {
            this.ask = ask;
            this.from = from;
        }

        @NotNull
        private static RF of(@NotNull final String value) {
            final List<String> values = Splitter.on('/').splitToList(value);
            if(values.size() != 2) {
                throw new IllegalArgumentException("Wrong replica factor:" + value);
            }
            return new RF(Integer.parseInt(values.get(0)), Integer.parseInt(values.get(1)));
        }
    }
}
