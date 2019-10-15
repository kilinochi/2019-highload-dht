package ru.mail.polis.exception;

import org.jetbrains.annotations.NotNull;

import java.util.NoSuchElementException;


/**
 * Custom exception.
 * */

@SuppressWarnings ("serial")
public final class NoSuchElementLite extends NoSuchElementException {

    public NoSuchElementLite(@NotNull final String s) {
        super(s);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        synchronized (this){
            return this;
        }
    }
}