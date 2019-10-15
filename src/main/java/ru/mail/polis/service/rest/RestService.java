package ru.mail.polis.service.rest;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import one.nio.http.HttpSession;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpServer;
import one.nio.http.Path;
import one.nio.http.Response;
import one.nio.http.Request;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import one.nio.server.RejectedSessionException;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.rest.session.StorageSession;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static one.nio.http.Response.*;

public final class RestService extends HttpServer implements Service {

    private static final String ENTITIES_PATH = "/entities";
    private static final String ENTITY_PATH = "/entity";
    private static final String STATUS_PATH = "/status";

    private final DAO dao;
    private final Executor workers;

    /**
     * Create rest http-server.
     *
     * @param port is the port which server can be work
     * @param dao  is persistent dao
     */
    private RestService(final int port, @NotNull final DAO dao, @NotNull final Executor executor) throws IOException {
        super(getConfig(port));
        this.dao = dao;
        this.workers = executor;
    }

    /**
     * Get request by this url.
     */
    @Path("/v0" + STATUS_PATH)
    public Response status() {
        return Response.ok("OK");
    }

    @Override
    public HttpSession createSession(Socket socket) throws RejectedSessionException {
        return new StorageSession(socket, this);
    }

    @Override
    public void handleDefault(@NotNull final Request request,
                              @NotNull final HttpSession session) throws IOException {
        switch (request.getPath()) {
            case "v0/" + ENTITY_PATH:
                entity(request, session);
                return;
            case "v0/" + ENTITIES_PATH:
                entities(request, session);
                return;
            default:
                session.sendError(BAD_REQUEST, "Wrong path");
        }
    }

    private void entity(@NotNull final Request request,
                        @NotNull final HttpSession session) throws IOException {
        final String id = request.getParameter("id");
        if (id == null || id.isEmpty()) {
            session.sendError(BAD_REQUEST, Arrays.toString("Key not found".getBytes(Charsets.UTF_8)));
            return;
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    executeAsync(session, () -> get(key));
                    return;
                case Request.METHOD_DELETE:
                    executeAsync(session, new Action() {
                        @Override
                        public Response act() throws IOException {
                            return delete(key);
                        }
                    });
                    return;
                case Request.METHOD_PUT:
                    executeAsync(session, ()-> upsert(key, request.getBody()));
                    return;
                default:
                    session.sendError(METHOD_NOT_ALLOWED, "Wrong method");
            }
        } catch (IOException e) {
            session.sendError(INTERNAL_ERROR, "Something wrong");
        } catch (NoSuchElementException e) {
            session.sendError(NOT_FOUND, "Not found resource");
        }
    }

    private void entities(@NotNull final Request request,
                          @NotNull final HttpSession session) throws IOException {
        final String start = request.getParameter("start=");
        if(start == null || start.isEmpty()) {
            session.sendError(BAD_REQUEST, "No start");
            return;
        }
        if(request.getMethod() != Request.METHOD_GET) {
            session.sendError(METHOD_NOT_ALLOWED, "Wrong method");
            return;
        }
        String end = request.getParameter("end=");
        if(end != null && end.isEmpty()) {
            end = null;
        }
        try {
            final Iterator <Record> iterator =
                    dao.range(
                            ByteBuffer.wrap(start.getBytes()),
                            end == null ? null : ByteBuffer.wrap(end.getBytes()));
            ((StorageSession) session).stream(iterator);
        } catch (IOException e) {
            session.sendError(INTERNAL_ERROR, "Something wrong");
        }
    }

    @FunctionalInterface
    private interface Action {
        Response act() throws IOException;
    }

    public static RestService create(@NotNull final DAO dao,
                                     final int port) throws IOException {
        final Executor executor = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors(),
                new ThreadFactoryBuilder().setNameFormat("workers").build()
        );
        return new RestService(port, dao, executor);
    }

    private static HttpServerConfig getConfig(final int port) {
        if (port <= 1024 || port >= 65536) {
            throw new IllegalArgumentException("Invalid port");
        }
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        final HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        return config;
    }

    private void executeAsync(
            @NotNull final HttpSession session,
            @NotNull final Action action) {
        workers.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    session.sendResponse(action.act());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private Response upsert(
            @NotNull final ByteBuffer key,
            @NotNull final byte[] value) throws IOException {
        dao.upsert(key, ByteBuffer.wrap(value));
        return new Response(Response.CREATED, EMPTY);
    }

    private Response delete(@NotNull final ByteBuffer key) throws IOException {
        dao.remove(key);
        return new Response(Response.ACCEPTED, EMPTY);
    }

    private Response get(
            @NotNull final ByteBuffer key) throws IOException {
        final ByteBuffer value = dao.get(key);
        final ByteBuffer duplicate = value.duplicate();
        final byte[] body = new byte[duplicate.remaining()];
        duplicate.get(body);
        return new Response(Response.OK, body);
    }
}
