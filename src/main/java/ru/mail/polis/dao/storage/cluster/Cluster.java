package ru.mail.polis.dao.storage.cluster;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Comparator;

public final class Cluster {

    public static final Comparator<Cluster> COMPARATOR = Comparator.comparing(Cluster::getKey)
            .thenComparing(Cluster::getClusterValue);

    private final ByteBuffer key;
    private final ClusterValue clusterValue;

    /**
     * Cluster is a memory cell in file.
     *
     * @param key is the key of this cell by which we can find this Cluster
     * @param clusterValue is the value in this cell
     **/
    private Cluster(@NotNull final ByteBuffer key, @NotNull final ClusterValue clusterValue) {
        this.key = key;
        this.clusterValue = clusterValue;
    }

    public static Cluster of(@NotNull final ByteBuffer key,
                             @NotNull final ClusterValue value) {
        return new Cluster(key, value);
    }

    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }


    public ClusterValue getClusterValue() {
        return clusterValue;
    }
}
