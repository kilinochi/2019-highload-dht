package ru.mail.polis.service.topology.node;

import org.jetbrains.annotations.NotNull;

public final class VirtualNode implements Node {

    private final ServiceNode serviceNode;
    private final int replicaIndex;

    /**
     * Virtual node is range in ring of cluster nodes.
     * @param replicaIndex index of replica
     * @param serviceNode is physical node in this range
     */
    public VirtualNode(@NotNull final ServiceNode serviceNode,
                       final int replicaIndex) {
        this.serviceNode = serviceNode;
        this.replicaIndex = replicaIndex;
    }

    @Override
    public String key() {
        return serviceNode.key() + "-" + replicaIndex;
    }

    public boolean isVirtualNodeOf(@NotNull final ServiceNode pNode) {
        return serviceNode.key().equals(pNode.key());
    }

    public ServiceNode getServiceNode() {
        return serviceNode;
    }
}
