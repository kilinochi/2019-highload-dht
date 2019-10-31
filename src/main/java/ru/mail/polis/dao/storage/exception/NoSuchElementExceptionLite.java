package ru.mail.polis.dao.storage.exception;

import org.jetbrains.annotations.NotNull;

import java.util.NoSuchElementException;

public final class NoSuchElementExceptionLite extends NoSuchElementException {

    private static final long serialVersionUID = 1L;

    /**
     * Custom lite exception extends NoSuchElementException.
     *
     * @param mess message with problem cause
     */
    public NoSuchElementExceptionLite(@NotNull final String mess) {
        super(mess);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        synchronized (this) {
            return this;
        }
    }
}
