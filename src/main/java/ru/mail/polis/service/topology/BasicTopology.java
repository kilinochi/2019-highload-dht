package ru.mail.polis.service.topology;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;

public final class BasicTopology implements Topology<String> {

    @NotNull
    private final String me;

    @NotNull
    private final String[] nodes;

    /**
     * Simple topology for cluster.
     * @param nodes is all nodes in clusters
     * @param me is current cluster
     */
    public BasicTopology(
            @NotNull final Set<String> nodes,
            @NotNull final String me) {
        this.me = me;
        this.nodes = new String[nodes.size()];
        nodes.toArray(this.nodes);
        Arrays.sort(this.nodes);
    }

    @Override
    public boolean isMe(@NotNull final String node) {
        return this.me.equals(node);
    }

    @NotNull
    @Override
    public String primaryFor(@NotNull final ByteBuffer key) {
        final int hash = key.hashCode();
        final int node = (hash & Integer.MAX_VALUE) % nodes.length;
        return nodes[node];
    }

    @NotNull
    @Override
    public Set<String> all() {
        return Set.of(this.nodes);
    }
}
