package ru.mail.polis.utils;

import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.storage.cell.Cell;
import ru.mail.polis.dao.storage.cell.CellValue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;

import static ru.mail.polis.service.rest.RestController.TIMESTAMP_HEADER;

public final class CellUtils {
    private CellUtils() {}

    private static final Logger logger = LoggerFactory.getLogger(CellUtils.class);

    /**
     * Get CellValue from response.
     * @param response response from witch we should be get value.
     */
    @NotNull
    public static CellValue getFromResponse(@NotNull final Response response) throws IOException {
        final String timestamp = response.getHeader(TIMESTAMP_HEADER);
        logger.info("Status from response is {} and timestamp is {}", response.getStatus(), timestamp);
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
     * @param cells is iterator of cells
     * @param key is key by we get data
     */
    @NotNull
    public static CellValue value(final @NotNull ByteBuffer key,
                                  final @NotNull Iterator<Cell> cells) {

        logger.info("data has next : {} ", cells.hasNext());

        if (!cells.hasNext()) {
            return CellValue.absent();
        }

        final Cell cell = cells.next();

        if(!cell.getKey().equals(key)) {
            logger.info("Not equals with key, return absent value");
            return CellValue.absent();
        }

        final long timestamp = cell.getCellValue().getTimestamp();
        final ByteBuffer value = cell.getCellValue().getData();
        if (value == null) {
            logger.info("Value is null, return removed value");
            return CellValue.removed(timestamp);
        } else {
            logger.info("Value is present, return alive value");
            return CellValue.present(value, timestamp);
        }
    }
}
