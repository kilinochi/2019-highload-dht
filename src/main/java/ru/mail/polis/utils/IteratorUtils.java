package ru.mail.polis.utils;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.Iters;
import ru.mail.polis.dao.storage.cell.Cell;
import ru.mail.polis.dao.storage.cell.CellValue;
import ru.mail.polis.dao.storage.table.SSTable;
import ru.mail.polis.dao.storage.table.Table;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

public final class IteratorUtils {
    private IteratorUtils() {

    }

    /**
     * Simple helper to collapse data from tables.
     *
     * @param table  is table witch collapse their iters with another tables
     * @param tables is collection witch collapse theirs iters with table
     * @param from   is key from we get data
     */
    public static Iterator<Cell> data(@NotNull final Table table,
                                      @NotNull final NavigableMap<Long, SSTable> tables,
                                      @NotNull final ByteBuffer from) {
        final List<Iterator<Cell>> list = compose(table, tables, from);
        final Iterator<Cell> cellIterator = collapseEquals(list);
        return filterAlive(cellIterator);
    }

    /**
     * Return latestIterators with removed cells.
     *
     * @param table    is table witch collapse their iters with another tables
     * @param ssTables is collection witch collapse theirs iters with table
     * @param from     is key from we get data
     */
    public static Iterator<Cell> latestIter(@NotNull final Table table,
                                            @NotNull final NavigableMap<Long, SSTable> ssTables,
                                            @NotNull final ByteBuffer from) {
        final List<Iterator<Cell>> iteratorList = compose(table, ssTables, from);
        return collapseEquals(iteratorList);
    }

    /**
     * Compose data from ssTables.
     *
     * @param table    is table from witch we should be get Iterators by key
     * @param ssTables is other ssTables from witch we should be get Iterators by key
     * @param from     is key from witch we should be get data
     */
    private static List<Iterator<Cell>> compose(
            @NotNull final Table table,
            @NotNull final NavigableMap<Long, SSTable> ssTables,
            @NotNull final ByteBuffer from) {
        final List<Iterator<Cell>> list = new ArrayList<>();
        list.add(table.iterator(from));
        for (final Table fromOther : ssTables.values()) {
            list.add(fromOther.iterator(from));
        }
        return list;
    }

    /**
     * Collapse equals iterators.
     *
     * @param data is iterators witch we must be collapse
     */
    private static Iterator<Cell> collapseEquals(@NotNull final List<Iterator<Cell>> data) {
        return Iters.collapseEquals(Iterators.mergeSorted(data, Cell.COMPARATOR), Cell::getKey);
    }

    /**
     * Filter and get only alive cell.
     *
     * @param cellIterator is data which we should be filtered.
     */
    private static Iterator<Cell> filterAlive(@NotNull final Iterator<Cell> cellIterator) {
        return Iterators.filter(
                cellIterator, cell -> {
                    assert cell != null;
                    return cell.getCellValue().getState() != CellValue.State.REMOVED;
                }
        );
    }
}
