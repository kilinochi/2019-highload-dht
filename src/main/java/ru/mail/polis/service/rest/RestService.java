package ru.mail.polis.service.rest;

import com.google.common.base.Charsets;
import one.nio.http.HttpSession;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpServer;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Response;
import one.nio.http.Request;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public final class RestService extends HttpServer implements Service {

    private static final String ENTITY_PATH = "/entity";
    private static final String STATUS_PATH = "/status";

    private final DAO dao;

    /**
     * Create rest http-server.
     *
     * @param port is the port which server can be work
     * @param dao  is persistent dao
     */
    public RestService(final int port, @NotNull final DAO dao) throws IOException {
        super(getConfig(port));
        this.dao = dao;
    }

    /**
     * Get request by this url.
     */
    @Path("/v0" + STATUS_PATH)
    public Response status() {
        return new Response(Response.OK, Response.EMPTY);
    }

    /**
     * Get request by this url.
     *
     * @param id      is key by which
     * @param request is request method which client send to server
     */
    @Path("/v0" + ENTITY_PATH)
    public Response entity(@Param("id") final String id, final Request request) {
        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, "Key not found".getBytes(Charsets.UTF_8));
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        return createResponse(request, key);
    }

    private Response createResponse(@NotNull final Request request, @NotNull final ByteBuffer key) {
        try {
            final int method = request.getMethod();
            switch (method) {
                case Request.METHOD_GET:
                    final ByteBuffer value = dao.get(key);
                    final ByteBuffer duplicate = value.duplicate();
                    final byte[] body = new byte[duplicate.remaining()];
                    duplicate.get(body);
                    return new Response(Response.OK, body);
                case Request.METHOD_PUT:
                    dao.upsert(key, ByteBuffer.wrap(request.getBody()));
                    return new Response(Response.CREATED, Response.EMPTY);
                case Request.METHOD_DELETE:
                    dao.remove(key);
                    return new Response(Response.ACCEPTED, Response.EMPTY);
                default:
                    return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
            }
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, "Key not found".getBytes(Charsets.UTF_8));
        }
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

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }
}
