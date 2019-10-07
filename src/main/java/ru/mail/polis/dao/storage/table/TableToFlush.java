package ru.mail.polis.dao.storage.table;

public final class TableToFlush {
    private final long generation;
    private final Table table;
    private final boolean poisonPills;

    public TableToFlush(long generation, Table table,
                        final boolean poisonPills) {
        this.generation = generation;
        this.table = table;
        this.poisonPills = poisonPills;
    }

    public TableToFlush(long generation, Table table) {
        this(generation, table, false);
    }

    public long getGeneration() {
        return generation;
    }

    public Table getTable() {
        return table;
    }

    public boolean isPoisonPills() {
        return poisonPills;
    }
}
