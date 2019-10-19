package ru.mail.polis.dao.storage.exception;

import org.jetbrains.annotations.NotNull;

import java.util.NoSuchElementException;

@SuppressWarnings("serial")
public class NoSuchElementExceptionLite extends NoSuchElementException {

    /** Custom lite extends NoSuchElement.
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
