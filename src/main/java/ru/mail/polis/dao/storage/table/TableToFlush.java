package ru.mail.polis.dao.storage.table;

public class TableToFlush {
    private final long generation;
    private final Table table;

    public TableToFlush(long generation, Table table) {
        this.generation = generation;
        this.table = table;
    }

    public long getGeneration() {
        return generation;
    }

    public Table getTable() {
        return table;
    }
}
