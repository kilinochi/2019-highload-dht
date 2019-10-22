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
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.google.common.base.Charsets.UTF_8;

public final class ConsistingHashTopology implements Topology<ServiceNode> {

    private static final Logger logger = LoggerFactory.getLogger(ConsistingHashTopology.class);

    private final Set<ServiceNode> nodes;
    private final ServiceNode me;
    private final SortedMap<Long, VirtualNode> ring = new TreeMap<>();
    private final HashFunction hashFunction;

    public ConsistingHashTopology(@NotNull final Set<ServiceNode> nodes,
                                  @NotNull final ServiceNode node,
                                  final long virtualNodeCount) {
        this.me = node;
        this.nodes = nodes;
        this.hashFunction = new MD5Hash();
        for(final ServiceNode serviceNode : nodes) {
            addNode(serviceNode, virtualNodeCount);
        }
    }

    private void addNode(ServiceNode serviceNode, long vNodeCount) {
        if(vNodeCount < 0) {
            throw new IllegalArgumentException("Illegal virtual node counts : " + vNodeCount);
        }
        int existingReplicas = getExistingReplicas(serviceNode);
        for(int i = 0; i < vNodeCount; i++) {
            VirtualNode virtualNode = new VirtualNode(serviceNode, i + existingReplicas);
            ring.put(hashFunction.hash(ByteBuffer.wrap(virtualNode.key().getBytes(UTF_8))), virtualNode);
        }
    }

    private int getExistingReplicas(ServiceNode node) {
        int replicas = 0;
        for(final VirtualNode virtualNode : ring.values()) {
            if(virtualNode.isVirtualNodeOf(node)) {
                replicas = replicas + 1;
            }
        }
        return replicas;
    }

    @Override
    public boolean isMe(@NotNull ServiceNode node) {
        return me.key().equals(node.key());
    }

    @NotNull
    @Override
    public ServiceNode primaryFor(@NotNull ByteBuffer key) {
        final Long hashVal = hashFunction.hash(key.asReadOnlyBuffer());
        final SortedMap<Long, VirtualNode> tailMap = ring.tailMap(hashVal);
        final Long nodeHashVal = !tailMap.isEmpty() ? tailMap.firstKey() : ring.firstKey();
        return ring.get(nodeHashVal).getServiceNode();
    }

    @NotNull
    @Override
    public Set<ServiceNode> all() {
        return nodes;
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
            byte[] digest = messageDigest.digest();
            long hash = 0;
            for(int i = 0; i < 4; i++) {
                hash <<= 8;
                hash |= ((int) digest[i]);
            }
            return hash;
        }
    }
}
