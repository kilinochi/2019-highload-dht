package ru.mail.polis.utils;

import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.storage.cell.Cell;
import ru.mail.polis.dao.storage.cell.CellValue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;

import static ru.mail.polis.service.rest.RestService.TIMESTAMP_HEADER;

public final class CellUtils {
    private static final Logger logger = LoggerFactory.getLogger(CellUtils.class);

    private CellUtils() {}

    /**
     * Get CellValue from response.
     * @param response response from witch we should be get value.
     */
    @NotNull
    public static CellValue getFromResponse(@NotNull final Response response) throws IOException {
        final String timestamp = response.getHeader(TIMESTAMP_HEADER);
        if(response.getStatus() == 200) {
            if(timestamp == null) {
                throw new IllegalArgumentException("Wrong input data!");
            }
            return CellValue.present(
                    ByteBuffer.wrap(response.getBody()), Long.parseLong(timestamp)
            );
        } else if(response.getStatus() == 404) {
            if(timestamp == null) {
                return CellValue.absent();
            } else {
                return CellValue.removed(Long.parseLong(timestamp));
            }
        } else {
            throw new IOException("IOException while get response from nodes");
        }
    }

    /**
     * Merge cells and get latest value from collection.
     * @param values is collection from witch we should be get value
     */
    @NotNull
    public static CellValue merge(@NotNull final Collection<CellValue> values) {
        return values.stream()
                .filter(clusterValue -> clusterValue.getState() != CellValue.State.ABSENT)
                .max(Comparator.comparingLong(CellValue::getTimestamp))
                .orElseGet(CellValue::absent);
    }

    /**
     * Get latest value from storage.
     * @param storage is storage from we get data
     * @param key is key by we get data
     */
    @NotNull
    public static CellValue value(final ByteBuffer key,
                                  final @NotNull DAO storage) {

        final Iterator<Cell> cells = storage.cellIterator(key.asReadOnlyBuffer());

        logger.info("Cell state is :");
        if (!cells.hasNext()) {
            logger.info("{}", CellValue.State.ABSENT);
            return CellValue.absent();
        }

        final Cell cell = cells.next();

        if(!cell.getKey().equals(key)) {
            return CellValue.absent();
        }

        if (cell.getCellValue().getData() == null) {
            logger.info("{}", CellValue.State.REMOVED);
            return CellValue.removed(cell.getCellValue().getTimestamp());
        } else {
            logger.info("{}", CellValue.State.PRESENT);
            final byte[] body = BytesUtils.body(cell.getCellValue().getData());
            return CellValue.present(ByteBuffer.wrap(body), cell.getCellValue().getTimestamp());
        }
    }
}
