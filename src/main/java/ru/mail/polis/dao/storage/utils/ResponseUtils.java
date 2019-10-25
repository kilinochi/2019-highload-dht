package ru.mail.polis.dao.storage.utils;

import one.nio.http.HttpSession;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.storage.cell.CellValue;

import java.io.IOException;
import java.nio.ByteBuffer;

import static ru.mail.polis.service.rest.RestService.TIMESTAMP_HEADER;

public final class ResponseUtils {

    private static final Logger logger = LoggerFactory.getLogger(ResponseUtils.class);

    private ResponseUtils() {
    }

    /**
     * Send response to client.
     * @param session is current session
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
                logger.error("Error while send error {} ", ex.getMessage());
            }
        }
    }

    /**
     * Get response from client.
     * @param clusterValue is value witch we should be insert in body response
     * @param proxy mark current response as proxy response
     */
    @NotNull
    public static Response from(@NotNull final CellValue clusterValue,
                                 final boolean proxy) {
        final Response result;
        switch (clusterValue.getState()) {
            case REMOVED: {
                result = new Response(Response.NOT_FOUND, Response.EMPTY);
                if(proxy) {
                    result.addHeader(TIMESTAMP_HEADER + clusterValue.getTimestamp());
                }
                return result;
            }
            case PRESENT: {
                final ByteBuffer value = clusterValue.getData();
                final byte[] body = BytesUtils.body(value);
                result = new Response(Response.OK, body);
                if(proxy) {
                    result.addHeader(TIMESTAMP_HEADER + clusterValue.getTimestamp());
                }
                return result;
            }
            case ABSENT:{
                return new Response(Response.NOT_FOUND, Response.EMPTY);
            }
            default:
                throw new IllegalArgumentException("Wrong input data!");
        }
    }
}