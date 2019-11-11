package ru.mail.polis.utils;

import one.nio.http.HttpSession;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.storage.cell.Value;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

import static ru.mail.polis.utils.ConstUtils.TIMESTAMP_HEADER;

public final class ResponseUtils {

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
            logger.error("Error while send error ", e.getCause());
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
                    result.addHeader(TIMESTAMP_HEADER + ": " + cellValue.getTimestamp());
                }
                return result;
            }
            case PRESENT: {
                final ByteBuffer value = cellValue.getData();
                final byte[] body = BytesUtils.body(value);
                result = new Response(Response.OK, body);
                if (proxy) {
                    result.addHeader(TIMESTAMP_HEADER + ": " + cellValue.getTimestamp());
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

    @NotNull
    public static Response responseFromValues(@NotNull final Collection<Value> values) {
        final Value value = Value.merge(values);
        return from(value, false);
    }

    public static Response build(@NotNull final String code, @NotNull final byte[] body) {
        return new Response(code, body);
    }
}
