package ru.mail.polis.dao.storage.utils;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.Iters;
import ru.mail.polis.dao.storage.cluster.Cluster;
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
     * @param table is table witch collapse their iters with another tables
     * @param tables is collection witch collapse theirs iters with table
     * @param from is key from we get data
     * */
    public static Iterator<Cluster> data(@NotNull final Table table,
                                         @NotNull final NavigableMap <Long, Table> tables,
                                         @NotNull final ByteBuffer from) {
        final List <Iterator<Cluster>> list = compose(table, tables, from);
        final Iterator<Cluster> clusterIterator = collapseEquals(list);
        return filter(clusterIterator);
    }

    public static List <Iterator<Cluster>> compose(
            @NotNull final Table table,
            @NotNull final NavigableMap <Long, Table> tables,
            @NotNull final ByteBuffer from
    ){
        final List <Iterator<Cluster>> list = new ArrayList<>();
        for (final Table fromOther : tables.values()) {
            list.add(fromOther.iterator(from));
        }
        list.add(table.iterator(from));
        return list;
    }

    public static Iterator<Cluster> collapseEquals(@NotNull final List <Iterator<Cluster>> data) {
        return Iters.collapseEquals(Iterators.mergeSorted(data, Cluster.COMPARATOR), Cluster::getKey);
    }

    public static Iterator<Cluster> filter(@NotNull final Iterator<Cluster> clusters) {
        return Iterators.filter(
                clusters, cluster -> {
                    assert cluster != null;
                    return !cluster.getClusterValue().isTombstone();
                }
        );
    }
}
