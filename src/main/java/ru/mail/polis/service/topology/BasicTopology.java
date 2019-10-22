/*
package ru.mail.polis.service.topology;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.service.topology.node.Node;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;

public final class BasicTopology implements Topology<Node> {

    @NotNull
    private final Node me;

    @NotNull
    private final Node[] nodes;


    */
/**
     * Simple topology for cluster.
     * @param nodes is all nodes in clusters
     * @param me is current cluster
     *//*

    public BasicTopology(
            @NotNull final Set<String> nodes,
            @NotNull final Node me) {
        this.me = me;
        this.nodes = new Node[nodes.size()];
        nodes.toArray(this.nodes);
        Arrays.sort(this.nodes);
    }

    @Override
    public boolean isMe(@NotNull Node node) {
        return false;
    }

    @NotNull
    @Override
    public Node primaryFor(@NotNull final ByteBuffer key) {
        final int hash = key.hashCode();
        final int index = (hash & Integer.MAX_VALUE) % nodes.length;
        return nodes[index];
    }

    @NotNull
    @Override
    public Set<Node> all() {
        return Set.of(this.nodes);
    }
}*/
