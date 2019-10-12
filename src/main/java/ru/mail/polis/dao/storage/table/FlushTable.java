package ru.mail.polis.dao.storage.table;

import ru.mail.polis.dao.storage.cluster.Cluster;

import java.util.Iterator;

public final class FlushTable {
    private final long generation;
    private final Iterator<Cluster> data;
    private final boolean poisonPills;
    private final boolean compactionTable;

    FlushTable(final long generation,
               final Iterator<Cluster> data,
               final boolean poisonPills,
               final boolean compactionTable) {
        this.generation = generation;
        this.data = data;
        this.poisonPills = poisonPills;
        this.compactionTable = compactionTable;
    }

    FlushTable(final long generation, final Iterator<Cluster> data, final boolean compactionTable) {
        this(generation, data, false, compactionTable);
    }

    public long getGeneration() {
        return generation;
    }

    public Iterator<Cluster> data() {
        return data;
    }

    public boolean isPoisonPills() {
        return poisonPills;
    }

    public boolean isCompactionTable() {
        return compactionTable;
    }
}
