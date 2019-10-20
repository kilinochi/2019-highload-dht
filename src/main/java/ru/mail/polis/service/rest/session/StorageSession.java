package ru.mail.polis.service.rest.session;

import com.google.common.base.Charsets;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.storage.utils.BytesUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;

public final class StorageSession extends HttpSession {

    private static final Logger logger = LoggerFactory.getLogger(StorageSession.class);
    private static final Charset UTF_8 = Charsets.UTF_8;
    private static final byte[] CRLF = "\r\n".getBytes(Charsets.UTF_8);
    private static final byte[] DELIMITER = "\n".getBytes(Charsets.UTF_8);
    private static final byte[] EMPTY_CHUNK = "0\r\n\r\n".getBytes(Charsets.UTF_8);

    private Iterator<Record> data;

    /**
     * Custom session for write range of chunks ro socket.
     * @param socket is socket in witch will be write range of data
     * @param server is server in witch context data will be write data
     */
    public StorageSession(@NotNull final Socket socket,
                          @NotNull final HttpServer server) {
        super(socket, server);
    }


    /**
     * Range streaming data as Iterator to socket.
     * @param records is iterator as data for stream.
     */
    public void stream(@NotNull final Iterator <Record> records) throws IOException {
        this.data = records;
        final Response response = new Response(Response.OK);
        response.addHeader("Transfer-Encoding: chunked");
        writeResponse(response, false);
        next();
    }

    @Override
    protected void processWrite() throws Exception {
        super.processWrite();
        next();
    }

    private void next() throws IOException {
        if(data == null) {
            throw new IllegalStateException("");
        }
        while (data.hasNext() && queueHead == null) {
            final Record record = data.next();
            writeRecord(record);
        }
        if(!data.hasNext()) {
            write(EMPTY_CHUNK, 0, EMPTY_CHUNK.length);

            server.incRequestsProcessed();

            if((handling = pipeline.pollFirst()) != null) {
                if(handling == FIN) {
                    scheduleClose();
                } else {
                    try {
                        server.handleRequest(handling, this);
                    } catch (IOException e) {
                        logger.info("Error" + e.getMessage());
                    }
                }
            }
        }
    }

    private void writeRecord(@NotNull final Record record) throws IOException {
        final byte[] key = BytesUtils.toArray(record.getKey());
        final byte[] value = BytesUtils.toArray(record.getValue());
        // <key>'\n'<value>
        final int payloadLength = key.length + DELIMITER.length + value.length;
        final String size = Integer.toHexString(payloadLength);
        // <size>\r\n<payload>\r\n
        final int chunkLength = size.length() + CRLF.length + payloadLength + CRLF.length;
        //chunk
        final byte[] chunk = new byte[chunkLength];
        final ByteBuffer chunkBuffer = ByteBuffer.wrap(chunk);
        chunkBuffer.put(size.getBytes(UTF_8));
        chunkBuffer.put(CRLF);
        chunkBuffer.put(key);
        chunkBuffer.put(DELIMITER);
        chunkBuffer.put(value);
        chunkBuffer.put(CRLF);
        write(chunk, 0, chunk.length);
    }
}
