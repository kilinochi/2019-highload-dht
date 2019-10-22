package ru.mail.polis.service.topology;

public class VirtualNode implements Node {

    private final ServiceNode physicalNode;
    private final int replicaIndex;

    VirtualNode(ServiceNode physicalNode, int replicaIndex) {
        this.replicaIndex = replicaIndex;
        this.physicalNode = physicalNode;
    }

    @Override
    public String getKey() {
        return physicalNode.getKey() + "-" + replicaIndex;
    }

    boolean isVirtualNodeOf(ServiceNode pNode) {
        return physicalNode.getKey().equals(pNode.getKey());
    }

    ServiceNode getPhysicalNode() {
        return physicalNode;
    }
}
