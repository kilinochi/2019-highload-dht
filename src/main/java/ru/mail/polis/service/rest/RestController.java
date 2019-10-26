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
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.DaoService;
import ru.mail.polis.utils.BytesUtils;
import ru.mail.polis.utils.ResponseUtils;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.rest.session.StorageSession;
import ru.mail.polis.service.topology.Topology;
import ru.mail.polis.service.topology.node.ServiceNode;

public final class RestController extends HttpServer implements Service {
    public static final String TIMESTAMP_HEADER = "X-OK-Timestamp";
    public static final String PROXY_HEADER = "X-OK-Proxy: True";

    private static final Logger logger = LoggerFactory.getLogger(RestController.class);

    private final DaoService daoService;
    private final RF defaultRF;
    private final int nodesSize;

    /**
     * Create new instance of RestService for interaction with database.
     * @param config in config for server
     * @param dao is dao for interaction with database
     * @param nodes all nodes in cluster
     * @param me is current node
     */
    private RestController(
            @NotNull final HttpServerConfig config,
            @NotNull final DAO dao,
            @NotNull final Topology<ServiceNode> nodes,
            @NotNull final ServiceNode me) throws IOException {
        super(config);
        this.nodesSize = nodes.size();
        Map<String, HttpClient> pool = new HashMap<>();
        this.defaultRF = new RF(nodes.size() / 2 + 1 , nodes.size());
        for(final ServiceNode node : nodes.all()) {
            if(!node.equals(me)) {
                final String url = node.key();
                assert !pool.containsKey(node.key());
                pool.put(url, new HttpClient(new ConnectionString(url + "?timeout=100")));
            }
        }
        this.daoService = new DaoService(dao, pool, nodes, me);
    }

    /**
     * Build new instance of RestService.
     * @param port is port on witch Service will be running
     * @param dao is dao for interaction with database
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
                .endsWith(""+port))
                .findFirst().get();
        final HttpServerConfig httpServerConfig = new HttpServerConfig();
        httpServerConfig.acceptors = new AcceptorConfig[]{acceptorConfig};
        httpServerConfig.minWorkers = Runtime.getRuntime().availableProcessors();
        httpServerConfig.maxWorkers = Runtime.getRuntime().availableProcessors();
        return new RestController(httpServerConfig, dao, nodes, me);
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
            final Iterator<Record> recordIterator = daoService.range(BytesUtils.keyByteBuffer(start),
                    end == null ? null : BytesUtils.keyByteBuffer(end));
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

        final boolean proxied = request.getHeader(PROXY_HEADER) != null;
        
        RF rf;
        try {
            rf = replicas == null ? defaultRF : RF.of(replicas);
            if(rf.ask < 1 || rf.from < rf.ask || rf.from > nodesSize) {
                throw new IllegalArgumentException("From is too big!");
            }
        } catch (IllegalArgumentException e) {
            ResponseUtils.sendResponse(session, new Response(Response.BAD_REQUEST, "WrongRF".getBytes(Charsets.UTF_8)));
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
                asyncExecute(session, () -> delete(id, finalRf, proxied));
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
                } catch (IOException ioException) {
                    logger.error("Error while send response {}", ioException.getMessage());
                }
            }
        });
    }

    private Response upsert(
            @NotNull final String id,
            @NotNull final byte[] value,
            @NotNull final RF rf,
            final boolean isProxy) throws IOException {
        return daoService.upsert(id, value, rf.ask, rf.from, isProxy);
    }

    private Response delete(
            @NotNull final String id,
            @NotNull final RF rf,
            final boolean proxy) throws IOException {
        return daoService.delete(id, rf.ask, rf.from, proxy);
    }

    private Response get(
            @NotNull final String id,
            @NotNull final RF rf,
            final boolean proxy) throws IOException {
        return daoService.get(id, rf.ask, rf.from, proxy);
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
