package ru.mail.polis.dao.storage.cell;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Comparator;

public final class Cell {

    public static final Comparator<Cell> COMPARATOR = Comparator.comparing(Cell::getKey)
            .thenComparing(Cell::getValue).thenComparing(Cell::getGeneration, Comparator.reverseOrder());

    private final ByteBuffer key;
    private final Value value;
    private final long generation;

    /**
     * Cell is a memory cell in file.
     *
     * @param key        is the key of this cell by which we can find this Cluster
     * @param value  is the value in this cell
     * @param generation is generation of this cell
     */
    private Cell(@NotNull final ByteBuffer key,
                 @NotNull final Value value,
                 final long generation) {
        this.key = key;
        this.value = value;
        this.generation = generation;
    }

    public static Cell of(@NotNull final ByteBuffer key,
                          @NotNull final Value value,
                          final long generation) {
        return new Cell(key, value, generation);
    }

    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    public Value getValue() {
        return value;
    }

    private long getGeneration() {
        return generation;
    }
}
