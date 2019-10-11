package ru.mail.polis.exception;

import java.util.NoSuchElementException;


/**
 * Custom exception.
 * */

public class NoSuchElementLite extends NoSuchElementException {

    public NoSuchElementLite(String s) {
        super(s);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}