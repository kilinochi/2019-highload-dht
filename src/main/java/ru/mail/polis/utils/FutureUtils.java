package ru.mail.polis.utils;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpResponse;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

public final class FutureUtils {

    private static final Logger logger = LoggerFactory.getLogger(FutureUtils.class);

    private FutureUtils() {

    }

    public static CompletableFuture<Collection<HttpResponse<byte[]>>> getHttpResponses(
            @NotNull final Collection<CompletableFuture<HttpResponse<byte[]>>> futures, final int acks) {
        if(futures.size() < acks) {
            throw new IllegalArgumentException("Number of http-responses is  " + futures.size() + " but acks is = " + acks);
        }

        final int maxFails = futures.size() - acks;
        final AtomicLong fails = new AtomicLong();
        final List<HttpResponse<byte[]>> responses = new CopyOnWriteArrayList<>();
        final CompletableFuture<Collection<HttpResponse<byte[]>>> res = new CompletableFuture<>();

        final BiConsumer<HttpResponse<byte[]>, Throwable> biConsumer = (response, throwable) -> {
            if ((throwable != null || response == null) && fails.incrementAndGet() > maxFails) {
                res.complete(responses);
            } else if (!res.isDone() && response != null) {
                responses.add(response);
                if (responses.size() >= acks) {
                    res.complete(responses);
                }
            }
        };

        for (final CompletableFuture<HttpResponse<byte[]>> future : futures) {
            future.orTimeout(1, TimeUnit.SECONDS)
                    .whenComplete(biConsumer)
                    .exceptionally(throwable -> {
                        logger.error("ERROR while get FUTURE, ", throwable);
                        return null;
                    });
        }
        return res;
    }
}
