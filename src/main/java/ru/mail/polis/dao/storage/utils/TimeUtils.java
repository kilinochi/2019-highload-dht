package ru.mail.polis.dao.storage.utils;

public final class TimeUtils  {
    private static long lastTime;
    private static long counter;

    private TimeUtils() {
    }

    public static long getTimeNanos() {
        final long currTime = System.currentTimeMillis();
        if(currTime != lastTime) {
            lastTime = currTime;
            counter = 0;
        }
        return currTime * 1_000_000 + counter++;
    }
}
