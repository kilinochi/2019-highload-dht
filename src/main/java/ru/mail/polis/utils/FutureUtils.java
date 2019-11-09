package ru.mail.polis.utils;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class FutureUtils {

    private static final Logger logger = LoggerFactory.getLogger(FutureUtils.class);

    private FutureUtils(){}

    /**
     * Collapse and compose collection of futures to future of collection
     * @param futures is futures
     * @param acks is acks
     */
    @NotNull
    public static <T> CompletableFuture<Collection<T>> compose(
            @NotNull final Collection<CompletableFuture<T>> futures,
            final int acks) {
        if(futures.size() < acks) {
            throw new IllegalArgumentException("Number of expected responses = "
                    + futures.size() + " but acks is " + "= " + acks);
        }

        final int maxFails = futures.size() - acks;
        final AtomicInteger fails = new AtomicInteger(0);
        final Collection<T> valuesFromResponses = new ConcurrentLinkedQueue<>();
        final CompletableFuture<Collection<T>> result = new CompletableFuture<>();

        for (final CompletableFuture<T> future : futures) {
            future.orTimeout(1, TimeUnit.SECONDS)
                .whenCompleteAsync(
                        (value, throwable) -> {
                            if((throwable != null || value == null) && fails.incrementAndGet() > maxFails) {
                                result.complete(valuesFromResponses);
                            }
                            else if(!result.isDone() && value != null) {
                                valuesFromResponses.add(value);
                                if(valuesFromResponses.size() >= acks) {
                                    result.complete(valuesFromResponses);
                                }
                            }
                        }
                )
                .exceptionally(throwable -> {
                    logger.error("Error while merge futures");
                    return null;
                });
        }
        return result;
    }
}
