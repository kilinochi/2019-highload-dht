package ru.mail.polis.utils;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

public final class FutureUtils {

    private static final Logger logger = LoggerFactory.getLogger(FutureUtils.class);

    private FutureUtils() {
    }

    /**
     * Collapse and compose collection of futures to future of collection.
     *
     * @param futures is futures
     * @param ack       is acks
     */
    @NotNull
    public static <T> CompletableFuture<Collection<T>> compose(
            @NotNull final Collection<CompletableFuture<T>> futures,
            final int ack) {
        final var maxFail = futures.size() - ack;
        if (maxFail < 0) {
            throw new IllegalArgumentException("Number of requested futures is too big: " + ack);
        }

        final Collection<T> results = new ArrayList<>();
        final Collection<Throwable> errors = new ArrayList<>();
        final Lock lock = new ReentrantLock();

        final CompletableFuture<Collection<T>> resultFuture = new CompletableFuture<>();

        futures.forEach(future -> future.whenComplete((value, throwable) -> {
            if (resultFuture.isDone()) {
                return;
            }
            lock.lock();
            try {
                if (throwable != null) {
                    errors.add(throwable);
                    if (errors.size() > maxFail) {
                        resultFuture.completeExceptionally(throwable);
                    }
                    return;
                }
                if (results.size() >= ack) {
                    return;
                }
                results.add(value);
                if (results.size() == ack) {
                    resultFuture.complete(results);
                }
            } finally {
                lock.unlock();
            }
        }).thenApply(x -> null)
                .exceptionally(FutureUtils::logError));
        return resultFuture;
    }

    @Nullable
    private static Void logError(@NotNull final Throwable t) {
        logger.error("Unexpected error", t);
        return null;
    }
}
