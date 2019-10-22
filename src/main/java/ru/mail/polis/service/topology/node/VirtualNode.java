package ru.mail.polis.service.topology.node;

import org.jetbrains.annotations.NotNull;

public final class VirtualNode implements Node {

    private final ServiceNode serviceNode;
    private final int replicaIndex;

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
