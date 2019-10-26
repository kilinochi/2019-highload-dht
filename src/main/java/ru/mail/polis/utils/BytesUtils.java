package ru.mail.polis.utils;

import com.google.common.base.Charsets;
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

    /**
     *  Return key as ByteBuffer.
     * @param id  is key witch we should be convert to ByteBuffer
     */
    public static ByteBuffer keyByteBuffer(@NotNull final String id) {
        return ByteBuffer.wrap(getBytesFromKey(id));
    }

    /**
     * Transform ByteBuffer to array of bytes.
     * @param buffer is ByteBuffer which will be transform
     */
    public static byte[] toArray(@NotNull final ByteBuffer buffer) {
        final ByteBuffer copy = buffer.duplicate();
        final byte[] array = new byte[copy.remaining()];
        copy.get(array);
        return array;
    }

    /**
     * Get bytes as massive from kee string.
     * @param id is id from response
     */
    public static byte[] getBytesFromKey(@NotNull final String id) {
        return id.getBytes(Charsets.UTF_8);
    }

    /**
     * Return ByteBuffer as body.
     * @param value is value to bytes massive
     */
    public static byte[] body(@NotNull final ByteBuffer value) {
        final ByteBuffer duplicate = value.duplicate();
        final byte[] body = new byte[duplicate.remaining()];
        duplicate.get(body);
        return body;
    }
}
