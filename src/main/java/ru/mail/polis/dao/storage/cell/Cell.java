package ru.mail.polis.dao.storage.cell;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Comparator;

public final class Cell {

    public static final Comparator<Cell> COMPARATOR = Comparator.comparing(Cell::getKey)
            .thenComparing(Cell::getCellValue).thenComparing(Cell::getGeneration, Comparator.reverseOrder());

    private final ByteBuffer key;
    private final CellValue cellValue;
    private final long generation;

    /**
     * Cell is a memory cell in file.
     *
     * @param key        is the key of this cell by which we can find this Cluster
     * @param cellValue  is the value in this cell
     * @param generation is generation of this cell
     */
    private Cell(@NotNull final ByteBuffer key,
                 @NotNull final CellValue cellValue,
                 final long generation) {
        this.key = key;
        this.cellValue = cellValue;
        this.generation = generation;
    }

    public static Cell of(@NotNull final ByteBuffer key,
                          @NotNull final CellValue value,
                          final long generation) {
        return new Cell(key, value, generation);
    }

    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    public CellValue getCellValue() {
        return cellValue;
    }

    private long getGeneration() {
        return generation;
    }
}
