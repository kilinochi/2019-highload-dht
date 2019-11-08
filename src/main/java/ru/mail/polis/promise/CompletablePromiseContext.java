package ru.mail.polis.promise;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("FutureReturnValueIgnored")
final class CompletablePromiseContext {

    private CompletablePromiseContext(){}

    private static final ScheduledExecutorService SERVICE = Executors.newSingleThreadScheduledExecutor();

    static void schedule(Runnable r) {
        SERVICE.schedule(r, 20, TimeUnit.MILLISECONDS);
    }
}
