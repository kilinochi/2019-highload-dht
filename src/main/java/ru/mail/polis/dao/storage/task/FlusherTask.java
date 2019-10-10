package ru.mail.polis.dao.storage.task;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.storage.cluster.Cluster;
import ru.mail.polis.dao.storage.table.MemoryTablePool;
import ru.mail.polis.dao.storage.table.TableToFlush;

import java.io.IOException;
import java.util.Iterator;

public final class FlusherTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(FlusherTask.class);

    private final MemoryTablePool memoryTablePool;
    private final DAO dao;

    public FlusherTask(@NotNull final MemoryTablePool memoryTablePool,
                       @NotNull final DAO dao) {
        this.memoryTablePool = memoryTablePool;
        this.dao = dao;
    }


    @Override
    public void run() {
        boolean poisonReceived = false;
        while (!Thread.currentThread().isInterrupted() && !poisonReceived) {
            TableToFlush tableToFlush;
            try {
                logger.info("Prepare to flush in flusher task: " + this.toString());
                tableToFlush = memoryTablePool.tableToFlush();
                final Iterator<Cluster> data = tableToFlush.data();
                long currentGeneration = tableToFlush.getGeneration();
                poisonReceived = tableToFlush.isPoisonPills();
                if(!tableToFlush.isCompactionTable()) {
                    dao.flush(currentGeneration, false, data);
                    memoryTablePool.switchCompaction();
                } else {
                    dao.flush(currentGeneration, true, data);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                logger.info("Error :" + e.getMessage());
            }
        }
    }
}
