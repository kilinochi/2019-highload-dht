package ru.mail.polis.dao.storage.utils;

import java.nio.ByteBuffer;

public final class BytesUtils {
    private BytesUtils() {
    }

    /**
    * Bytes wrapper.
    * @param value is value which we should be wrap
    * */

    public static ByteBuffer fromInt(final int value) {
        final ByteBuffer result = ByteBuffer.allocate(Integer.BYTES);
        result.putInt(value);
        result.rewind();
        return result;
    }

    /**
     * Bytes wrapper.
     * @param value is value which we should be wrap
     * */

    public static ByteBuffer fromLong(final long value) {
        final ByteBuffer result = ByteBuffer.allocate(Long.BYTES);
        result.putLong(value);
        result.rewind();
        return result;
    }
}
