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
                                         @NotNull final NavigableMap <Long, SSTable> tables,
                                         @NotNull final ByteBuffer from) {
        final List <Iterator<Cluster>> list = compose(table, tables, from);
        final Iterator<Cluster> clusterIterator = collapseEquals(list);
        return filter(clusterIterator);
    }

    /**
     * Compose data from ssTables.
     * @param table is table from witch we should be get Iterators by key
     * @param ssTables is other ssTables from witch we should be get Iterators by key
     * @param from is key from witch we should be get data
     * */
    private static List <Iterator<Cluster>> compose(
            @NotNull final Table table,
            @NotNull final NavigableMap<Long, SSTable> ssTables,
            @NotNull final ByteBuffer from){
        final List <Iterator<Cluster>> list = new ArrayList<>();
        for (final Table fromOther : ssTables.values()) {
            list.add(fromOther.iterator(from));
        }
        list.add(table.iterator(from));
        return list;
    }

    /**
     * Collapse equals iterators.
     * @param data is iterators witch we must be collapse
     * */
    private static Iterator<Cluster> collapseEquals(@NotNull final List<Iterator<Cluster>> data) {
        return Iters.collapseEquals(Iterators.mergeSorted(data, Cluster.COMPARATOR), Cluster::getKey);
    }
    
    /**
     * Filter and get only alive Clusters.
     * @param clusters is data which we should be filtered.
     */
    private static Iterator<Cluster> filter(@NotNull final Iterator<Cluster> clusters) {
        return Iterators.filter(
                clusters, cluster -> {
                    assert cluster != null;
                    return !cluster.getClusterValue().isTombstone();
                }
        );
    }
}
