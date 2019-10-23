package ru.mail.polis.service.topology;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.service.topology.hash.HashFunction;
import ru.mail.polis.service.topology.node.ServiceNode;
import ru.mail.polis.service.topology.node.VirtualNode;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static com.google.common.base.Charsets.UTF_8;

public final class ConsistingHashTopology implements Topology<ServiceNode> {

    private static final Logger logger = LoggerFactory.getLogger(ConsistingHashTopology.class);

    private final Set<ServiceNode> nodes;
    private final ServiceNode me;
    private final SortedMap<Long, VirtualNode> ring = new TreeMap<>();
    private final HashFunction hashFunction;

    /**
     * Create topology based on Consisting hashing.
     * @param nodes is all nodes in cluster
     * @param node is current node.
     * @param virtualNodeCount  is virtual node count in ring.
     */
    ConsistingHashTopology(@NotNull final Set<ServiceNode> nodes,
                           @NotNull final ServiceNode node,
                           final long virtualNodeCount) {
        this.me = node;
        this.nodes = nodes;
        this.hashFunction = new MD5Hash();
        for(final ServiceNode serviceNode : nodes) {
            addNode(serviceNode, virtualNodeCount);
        }
    }

    private void addNode(@NotNull final ServiceNode serviceNode,
                         final long vNodeCount) {
        if(vNodeCount < 0) {
            throw new IllegalArgumentException("Illegal virtual node counts : " + vNodeCount);
        }
        final int existingReplicas = getExistingReplicas(serviceNode);
        for(int i = 0; i < vNodeCount; i++) {
            final VirtualNode virtualNode = new VirtualNode(serviceNode, i + existingReplicas);
            ring.put(hashFunction.hash(ByteBuffer.wrap(virtualNode.key().getBytes(UTF_8))), virtualNode);
        }
    }

    private int getExistingReplicas(@NotNull final ServiceNode node) {
        int replicas = 0;
        for(final VirtualNode virtualNode : ring.values()) {
            if(virtualNode.isVirtualNodeOf(node)) {
                replicas = replicas + 1;
            }
        }
        return replicas;
    }

    private SortedMap<Long, VirtualNode> tailMap(@NotNull final ByteBuffer key) {
        Long hashVal = hashFunction.hash(key.asReadOnlyBuffer());
        return ring.tailMap(hashVal);
    }

    @Override
    public boolean isMe(@NotNull final ServiceNode node) {
        return me.key().equals(node.key());
    }

    @NotNull
    @Override
    public ServiceNode primaryFor(@NotNull final ByteBuffer key) {
        final SortedMap<Long, VirtualNode> tailMap = tailMap(key);
        final Long nodeHashVal;
        if(tailMap.isEmpty()) {
            nodeHashVal = ring.firstKey();
        } else {
            nodeHashVal = tailMap.firstKey();
        }
        return ring.get(nodeHashVal).getServiceNode();
    }

    @NotNull
    @Override
    public Set<ServiceNode> all() {
        return nodes;
    }

    @Override
    public long size() {
        return nodes.size();
    }

    @NotNull
    @Override
    public ServiceNode[] replicas(@NotNull final ByteBuffer key) {
        final SortedMap<Long, VirtualNode> tailMap = tailMap(key);
        return tailMap.values()
                .stream()
                .map(VirtualNode::getServiceNode)
                .distinct()
                .sorted()
                .toArray(ServiceNode[]::new);
    }

    private static final class MD5Hash implements HashFunction {

        private MessageDigest messageDigest;

        private MD5Hash() {
            try {
                messageDigest = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                logger.error("Exception : " + e.getMessage());
            }
        }

        @Override
        public long hash(@NotNull final ByteBuffer key) {
            messageDigest.reset();
            final ByteBuffer duplicate = key.duplicate();
            final byte[] bytes = new byte[duplicate.remaining()];
            key.get(bytes);
            messageDigest.update(bytes);
            final byte[] digest = messageDigest.digest();
            long hash = 0;
            for(int i = 0; i < 4; i++) {
                hash <<= 8;
                hash |= digest[i];
            }
            return hash;
        }
    }
}
