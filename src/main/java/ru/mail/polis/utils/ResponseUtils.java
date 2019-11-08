package ru.mail.polis.utils;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.storage.cell.Value;

import java.io.IOException;
import java.nio.ByteBuffer;

import static ru.mail.polis.service.rest.RestController.TIMESTAMP_HEADER;

public final class ResponseUtils {

    static final String PROXY_HEADER = "X-OK-Proxy";

    private static final Logger logger = LoggerFactory.getLogger(ResponseUtils.class);

    private ResponseUtils() {
    }

    /**
     * Send response to client.
     *
     * @param session  is current session
     * @param response is response witch we should be insert in session
     */
    public static void sendResponse(@NotNull final HttpSession session,
                                    @NotNull final Response response) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            try {
                session.sendError(Response.INTERNAL_ERROR, "Error while send response");
            } catch (IOException ex) {
                logger.error("Error while send error ", ex.getCause());
            }
        }
    }

    /**
     * Get response from client.
     *
     * @param cellValue is value witch we should be insert in body response
     * @param proxy     mark current response as proxy response
     */
    @NotNull
    public static Response from(@NotNull final Value cellValue,
                                final boolean proxy) {
        final Response result;
        final Value.State state = cellValue.getState();
        switch (state) {
            case REMOVED: {
                result = new Response(Response.NOT_FOUND, Response.EMPTY);
                if (proxy) {
                    logger.info("response is prosy, value is removed, add ts is header {}", cellValue.getTimestamp());
                    result.addHeader(TIMESTAMP_HEADER + cellValue.getTimestamp());
                }
                return result;
            }
            case PRESENT: {
                final ByteBuffer value = cellValue.getData();
                final byte[] body = BytesUtils.body(value);
                result = new Response(Response.OK, body);
                if (proxy) {
                    logger.info("response is prosy, value is alive, add ts is header {}", cellValue.getTimestamp());
                    result.addHeader(TIMESTAMP_HEADER + cellValue.getTimestamp());
                }
                return result;
            }
            case ABSENT: {
                return new Response(Response.NOT_FOUND, Response.EMPTY);
            }
            default:
                throw new IllegalArgumentException("Wrong input data!");
        }
    }

    public static Response build(@NotNull final String code, @NotNull final byte[] body) {
        return new Response(code, body);
    }

    public static boolean isProxied(@NotNull final Request request) {
        return request.getHeader(PROXY_HEADER) != null;
    }
}
