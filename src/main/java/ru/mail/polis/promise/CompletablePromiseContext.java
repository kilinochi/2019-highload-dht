package ru.mail.polis.promise;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("FutureReturnValueIgnored")
final class CompletablePromiseContext {

    private static final ScheduledExecutorService SERVICE = Executors.newSingleThreadScheduledExecutor();

    private CompletablePromiseContext(){}

    static void schedule(@NotNull final Runnable r) {
        SERVICE.schedule(r, 20, TimeUnit.MILLISECONDS);
    }
}
