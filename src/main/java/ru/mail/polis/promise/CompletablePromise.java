package ru.mail.polis.promise;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public final class CompletablePromise<V> extends CompletableFuture<V> {
    private final Future<V> future;

    public CompletablePromise(@NotNull final Future<V> future) {
        this.future = future;
        CompletablePromiseContext.schedule(this::tryToComplete);
    }

    private void tryToComplete() {
        if (future.isDone()) {
            try {
                complete(future.get());
            } catch (InterruptedException e) {
                completeExceptionally(e);
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                completeExceptionally(e.getCause());
            }
            return;
        }

        if (future.isCancelled()) {
            cancel(true);
            return;
        }

        CompletablePromiseContext.schedule(this::tryToComplete);
    }
}
