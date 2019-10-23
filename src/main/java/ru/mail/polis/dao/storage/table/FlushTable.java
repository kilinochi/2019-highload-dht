package ru.mail.polis.dao.storage.table;

import ru.mail.polis.dao.storage.cell.Cell;

import java.util.Iterator;

public final class FlushTable {
    private final long generation;
    private final Iterator<Cell> data;
    private final boolean poisonPills;

    FlushTable(final long generation,
               final Iterator<Cell> data,
               final boolean poisonPills) {
        this.generation = generation;
        this.data = data;
        this.poisonPills = poisonPills;
    }

    public long getGeneration() {
        return generation;
    }

    public Iterator<Cell> data() {
        return data;
    }

    public boolean isPoisonPills() {
        return poisonPills;
    }
}
