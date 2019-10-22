package ru.mail.polis.service.topology;

public final class VirtualNode<T extends Node> implements Node {

    private final T physicalNode;
    private final int replicaIndex;

    VirtualNode(T physicalNode, int replicaIndex) {
        this.replicaIndex = replicaIndex;
        this.physicalNode = physicalNode;
    }

    @Override
    public String url() {
        return physicalNode.url() + "-" + replicaIndex;
    }

    boolean isVirtualNodeOf(ServiceNode pNode) {
        return physicalNode.url().equals(pNode.url());
    }

    T getPhysicalNode() {
        return physicalNode;
    }
}
