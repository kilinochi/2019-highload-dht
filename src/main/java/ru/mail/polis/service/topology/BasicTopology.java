package ru.mail.polis.service.topology;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.service.topology.node.ServiceNode;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;

public final class BasicTopology implements Topology<ServiceNode> {

    @NotNull
    private final ServiceNode me;

    @NotNull
    private final ServiceNode[] nodes;

    /**
     * Simple topology for cluster.
     *
     * @param nodes is all nodes in clusters
     * @param me    is current cluster
     */
    BasicTopology(
            @NotNull final Set<ServiceNode> nodes,
            @NotNull final ServiceNode me) {
        this.me = me;
        this.nodes = new ServiceNode[nodes.size()];
        nodes.toArray(this.nodes);
        Arrays.sort(this.nodes);
    }

    @Override
    public boolean isMe(@NotNull final ServiceNode node) {
        return this.me.key().equals(node.key());
    }

    @NotNull
    @Override
    public ServiceNode primaryFor(@NotNull final ByteBuffer key) {
        final int hash = key.hashCode();
        final int index = (hash & Integer.MAX_VALUE) % nodes.length;
        return nodes[index];
    }

    @NotNull
    @Override
    public Set<ServiceNode> all() {
        return Set.of(this.nodes);
    }

    @Override
    public int size() {
        return nodes.length;
    }

    @NotNull
    @Override
    public ServiceNode[] replicas(final int count, @NotNull final ByteBuffer key) {
        final ServiceNode[] res = new ServiceNode[count];
        int index = (key.hashCode() & Integer.MAX_VALUE) % nodes.length;
        for (int j = 0; j < count; j++) {
            res[j] = nodes[index];
            index = (index + 1) % nodes.length;
        }
        return res;
    }
}
