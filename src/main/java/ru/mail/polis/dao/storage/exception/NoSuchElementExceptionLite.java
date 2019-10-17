package ru.mail.polis.dao.storage.exception;

import java.util.NoSuchElementException;

@SuppressWarnings("serial")
public final class NoSuchElementExceptionLite extends NoSuchElementException {

    /**
     * Custom NoSuchElementException.
     * @param mess is cause message
     */
    public NoSuchElementExceptionLite(final String mess) {
        super();
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        synchronized (this){
            return this;
        }
    }
}
