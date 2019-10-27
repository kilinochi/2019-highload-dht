package ru.mail.polis.dao.storage.table;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.utils.BytesUtils;
import ru.mail.polis.dao.storage.cell.Cell;
import ru.mail.polis.dao.storage.cell.CellValue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class SSTable implements Table {

    private final int rows;
    private final LongBuffer offsets;
    private final ByteBuffer clusters;
    private final File table;
    private final long currentGeneration;

    /**
     * Write data as iterator in disk.
     *
     * @param clusters is the data, which we want to write
     * @param to       is the file in the directory in which we want
     *                 write data
     */
    public static void writeToFile(@NotNull final Iterator<Cell> clusters, @NotNull final File to)
            throws IOException {
        try (FileChannel fileChannel = FileChannel.open(
                to.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            final List<Long> offsets = new ArrayList<>();
            long offset = 0;
            while (clusters.hasNext()) {
                offsets.add(offset);

                final Cell cell = clusters.next();

                // Write Key
                final ByteBuffer key = cell.getKey();
                final int keySize = cell.getKey().remaining();
                fileChannel.write(BytesUtils.fromInt(keySize));
                offset += Integer.BYTES; // 4 byte
                final ByteBuffer keyDuplicate = key.duplicate();
                fileChannel.write(keyDuplicate);
                offset += keySize;

                // Value
                final CellValue value = cell.getCellValue();

                // Write Timestamp
                if (value.getState() == CellValue.State.REMOVED) {
                    fileChannel.write(BytesUtils.fromLong(-cell.getCellValue().getTimestamp()));
                } else {
                    fileChannel.write(BytesUtils.fromLong(cell.getCellValue().getTimestamp()));
                }
                offset += Long.BYTES; // 8 byte

                // Write Value Size and Value

                if (value.getState() != CellValue.State.REMOVED) {
                    final ByteBuffer valueData = value.getData();
                    final int valueSize = value.getData().remaining();
                    fileChannel.write(BytesUtils.fromInt(valueSize));
                    offset += Integer.BYTES; // 4 byte
                    fileChannel.write(valueData);
                    offset += valueSize;
                }
                //else - not write Value
            }
            // Write Offsets
            for (final Long anOffset : offsets) {
                fileChannel.write(BytesUtils.fromLong(anOffset));
            }
            //Cells
            fileChannel.write(BytesUtils.fromLong(offsets.size()));
        }
    }

    /**
     * File mapping from disk.
     *
     * @param file is the file from which we read data
     **/
    public SSTable(@NotNull final File file, final long currentGeneration) throws IOException {
        final long fileSize = file.length();
        final ByteBuffer mapped;
        try (FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            assert fileSize <= Integer.MAX_VALUE;
            mapped = fc.map(FileChannel.MapMode.READ_ONLY, 0L, fileSize).order(ByteOrder.BIG_ENDIAN);
        }
        // Rows
        final long rowsValue = mapped.getLong((int) (fileSize - Long.BYTES)); //fileSize - 8 byte
        assert rowsValue <= Integer.MAX_VALUE;
        this.rows = (int) rowsValue;

        // Offset
        final ByteBuffer offsetBuffer = mapped.duplicate();
        offsetBuffer.position(mapped.limit() - Long.BYTES * rows - Long.BYTES);
        offsetBuffer.limit(mapped.limit() - Long.BYTES);
        this.offsets = offsetBuffer.slice().asLongBuffer();

        // Clusters
        final ByteBuffer clusterBuffer = mapped.duplicate();
        clusterBuffer.limit(offsetBuffer.position());
        this.clusters = clusterBuffer.slice();
        this.table = file;
        this.currentGeneration = currentGeneration;
    }

    /**
     * Iterator of data from file.
     *
     * @param from is the key, which help to find necessary
     *             clusters of data
     **/
    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return new Iterator<>() {

            int next = position(from);

            @Override
            public boolean hasNext() {
                return next < rows;
            }

            @Override
            public Cell next() {
                assert hasNext();
                return clusterAt(next++);
            }
        };
    }

    @Override
    public void upsert(final @NotNull ByteBuffer key, final @NotNull ByteBuffer value) {
        throw new UnsupportedOperationException("Not upsert!");
    }

    @Override
    public void remove(final @NotNull ByteBuffer key) {
        throw new UnsupportedOperationException("Not remove!");
    }

    @Override
    public long generation() {
        return currentGeneration;
    }

    @Override
    public long size() {
        return 0;
    }

    public File getTable() {
        return table;
    }

    private int position(final @NotNull ByteBuffer from) {
        int left = 0;
        int right = rows - 1;
        while (left <= right) {
            final int mid = left + (right - left) / 2;
            final int cmp = from.compareTo(keyAt(mid));
            if (cmp < 0) {
                right = mid - 1;
            } else if (cmp > 0) {
                left = mid + 1;
            } else {
                return mid;
            }
        }
        return left;
    }

    private ByteBuffer keyAt(final int i) {
        assert 0 <= i && i < rows;
        final long offset = offsets.get(i);
        assert offset <= Integer.MAX_VALUE;
        final int keySize = clusters.getInt((int) offset);
        final ByteBuffer key = clusters.duplicate();
        key.position((int) (offset + Integer.BYTES));
        key.limit(key.position() + keySize);
        return key.slice();
    }

    private Cell clusterAt(final int i) {
        assert 0 <= i && i < rows;
        long offset = offsets.get(i);
        assert offset <= Integer.MAX_VALUE;

        //Key
        final int keySize = clusters.getInt((int) offset);
        offset += Integer.BYTES;
        final ByteBuffer key = clusters.duplicate();
        key.position((int) offset);
        key.limit(key.position() + keySize);
        offset += keySize;

        //Timestamp
        final long timeStamp = clusters.getLong((int) offset);
        offset += Long.BYTES;

        if (timeStamp < 0) {
            return Cell.of(key.slice(),
                    new CellValue(null, CellValue.State.REMOVED , -timeStamp),
                    currentGeneration);
        } else {
            final int valueSize = clusters.getInt((int) offset);
            offset += Integer.BYTES;
            final ByteBuffer value = clusters.duplicate();
            value.position((int) offset);
            value.limit(value.position() + valueSize)
                    .position((int) offset)
                    .limit((int) (offset + valueSize));
            return Cell.of(key.slice(),
                    new CellValue(value.slice(),
                            CellValue.State.PRESENT, timeStamp), currentGeneration);
        }
    }
}
