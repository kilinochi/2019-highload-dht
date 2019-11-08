package ru.mail.polis.utils;

public final class ConstUtils {
    private ConstUtils(){}

    public static final String TIMESTAMP_HEADER = "X-OK-Timestamp";
    public static final String PROXY_HEADER_NAME = "X-OK-Proxy";
    public static final String PROXY_HEADER_VALUE = "True";
    public static final String PROXY_HEADER = PROXY_HEADER_NAME + ": " + PROXY_HEADER_VALUE;
}
