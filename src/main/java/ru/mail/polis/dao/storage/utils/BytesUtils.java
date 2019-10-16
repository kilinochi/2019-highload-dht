package ru.mail.polis.dao.storage.utils;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class BytesUtils {
    private BytesUtils() {
    }

    /**
    * Bytes wrapper.
    * @param value is value which we should be wrap
    */
    public static ByteBuffer fromInt(final int value) {
        final ByteBuffer result = ByteBuffer.allocate(Integer.BYTES);
        result.putInt(value);
        result.rewind();
        return result;
    }

    /**
     * Bytes wrapper.
     * @param value is value which we should be wrap
     */
    public static ByteBuffer fromLong(final long value) {
        final ByteBuffer result = ByteBuffer.allocate(Long.BYTES);
        result.putLong(value);
        result.rewind();
        return result;
    }

    public static byte[] toArray(@NotNull final ByteBuffer buffer) {
        final ByteBuffer copy = buffer.duplicate();
        final byte[] array = new byte[copy.remaining()];
        copy.get(array);
        return array;
    }
}
