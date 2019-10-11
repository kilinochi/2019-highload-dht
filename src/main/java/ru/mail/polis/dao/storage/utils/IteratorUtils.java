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
     * @param sstables is collection witch collapse theirs iters with table
     * @param from is key from we get data
     * */
    public static Iterator<Cluster> data(@NotNull final Table table,
                                         @NotNull final NavigableMap <Long, SSTable> sstables,
                                         @NotNull final ByteBuffer from) {
        final List <Iterator<Cluster>> list = new ArrayList<>();
        for (final Table fromOther : sstables.values()) {
                list.add(fromOther.iterator(from));
        }
        list.add(table.iterator(from));
        final Iterator<Cluster> clusterIterator = Iters.collapseEquals(
                Iterators.mergeSorted(list, Cluster.COMPARATOR),
                Cluster::getKey
        );
        return Iterators.filter(
                clusterIterator, cluster -> {
                    assert cluster != null;
                    return !cluster.getClusterValue().isTombstone();
                }
        );
    }
}
