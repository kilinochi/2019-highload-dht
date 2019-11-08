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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Iterator;

import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.rest.service.EntityService;
import ru.mail.polis.utils.BytesUtils;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.rest.session.StorageSession;
import ru.mail.polis.service.topology.Topology;
import ru.mail.polis.service.topology.node.ServiceNode;

import static ru.mail.polis.utils.ResponseUtils.build;
import static ru.mail.polis.utils.ResponseUtils.sendResponse;

public final class RestController extends HttpServer implements Service {
    public static final String TIMESTAMP_HEADER = "X-OK-Timestamp";
    private static final String PROXY_HEADER = "X-OK-Proxy: True";

    private static final Logger logger = LoggerFactory.getLogger(RestController.class);

    private final RF defaultRF;
    private final long nodesSize;
    private final EntityService entityService;

    /**
     * Create new instance of RestService for interaction with database.
     *
     * @param config in config for server
     * @param dao    is dao for interaction with database
     * @param nodes  all nodes in cluster
     */
    private RestController(
            @NotNull final HttpServerConfig config,
            @NotNull final DAO dao,
            @NotNull final Topology<ServiceNode> nodes) throws IOException {
        super(config);
        this.nodesSize = nodes.size();
        this.defaultRF = new RF(nodes.size() / 2 + 1, nodes.size());
        this.entityService = new EntityService(dao, nodes);
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

        final HttpServerConfig httpServerConfig = new HttpServerConfig();
        httpServerConfig.acceptors = new AcceptorConfig[]{acceptorConfig};
        httpServerConfig.minWorkers = Runtime.getRuntime().availableProcessors() + 1;
        httpServerConfig.maxWorkers = Runtime.getRuntime().availableProcessors() + 1;
        return new RestController(httpServerConfig, dao, nodes);
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
            final Iterator<Record> recordIterator = entityService.range(BytesUtils.keyByteBuffer(start),
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


        final ByteBuffer key = BytesUtils.keyByteBuffer(id);
        boolean proxied = false;
        if (request.getHeader(PROXY_HEADER) != null) {
            proxied = true;
        }
        boolean finalProxied = proxied;
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

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                asyncExecute(() -> entityService.get(session, id, key, rf.ack, rf.from, finalProxied));
                break;
            case Request.METHOD_DELETE:
                asyncExecute(() -> entityService.delete(session, id, key, rf.ack, rf.from, finalProxied));
                break;
            case Request.METHOD_PUT:
                asyncExecute(() -> entityService.upsert(session, id, key, request.getBody(), rf.ack, rf.from, finalProxied));
                break;
            default:
                sendResponse(session, new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY));
                break;
        }
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
