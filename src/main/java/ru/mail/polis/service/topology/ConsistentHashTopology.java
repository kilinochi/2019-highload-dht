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
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;

import static com.google.common.base.Charsets.UTF_8;

public final class ConsistentHashTopology implements Topology<ServiceNode> {

    private static final Logger logger = LoggerFactory.getLogger(ConsistentHashTopology.class);

    private final Set<ServiceNode> nodes;
    private final ServiceNode me;
    private final SortedMap<Long, VirtualNode> ring = new TreeMap<>();
    private final HashFunction hashFunction;

    /**
     * Create topology based on Consisting hashing.
     *
     * @param nodes            is all nodes in cluster
     * @param me               is current node
     * @param virtualNodeCount is range in ring
     */
    ConsistentHashTopology(@NotNull final Set<ServiceNode> nodes,
                           @NotNull final ServiceNode me,
                           final long virtualNodeCount) {
        this.me = me;
        this.nodes = nodes;
        this.hashFunction = new MD5Hash();
        nodes.forEach(node -> addNode(node, virtualNodeCount));
    }

    private void addNode(@NotNull final ServiceNode serviceNode,
                         final long vNodeCount) {
        if (vNodeCount < 0) {
            throw new IllegalArgumentException("Illegal virtual node counts : " + vNodeCount);
        }
        final int existingReplicas = getExistingReplicas(serviceNode);
        for (int i = 0; i < vNodeCount; i++) {
            final VirtualNode virtualNode = new VirtualNode(serviceNode, i + existingReplicas);
            ring.put(hashFunction.hash(ByteBuffer.wrap(virtualNode.key().getBytes(UTF_8))), virtualNode);
        }
    }

    private int getExistingReplicas(@NotNull final ServiceNode node) {
        int replicas = 0;
        for (final VirtualNode virtualNode : ring.values()) {
            if (virtualNode.isVirtualNodeOf(node)) {
                replicas = replicas + 1;
            }
        }
        return replicas;
    }

    private SortedMap<Long, VirtualNode> tailMap(@NotNull final ByteBuffer key) {
        final Long hashVal = hashFunction.hash(key.asReadOnlyBuffer());
        return ring.tailMap(hashVal);
    }

    @Override
    public int size() {
        return nodes.size();
    }

    @NotNull
    @Override
    public List<ServiceNode> replicas(final int count,
                                  @NotNull final ByteBuffer key) {
        final SortedMap<Long, VirtualNode> tailMap = tailMap(key);
        final List<ServiceNode> nodesTailMap =
                tailMap.values()
                        .stream()
                        .map(VirtualNode::getServiceNode)
                        .distinct()
                        .limit(count)
                        .sorted()
                        .collect(Collectors.toCollection(ArrayList::new));
        if (nodesTailMap.size() < count) {
            final long limit = (long) count - nodesTailMap.size();

            final List<ServiceNode> startHeadNodes =
                    ring.tailMap(ring.firstKey())
                            .values()
                            .stream()
                            .map(VirtualNode::getServiceNode)
                            .distinct()
                            .limit(limit)
                            .sorted()
                            .collect(Collectors.toCollection(ArrayList::new));
            nodesTailMap.addAll(startHeadNodes);
        }
        return nodesTailMap;
    }

    @NotNull
    @Override
    public ServiceNode whoAmI() {
        return me;
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
        if (tailMap.isEmpty()) {
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

    private static final class MD5Hash implements HashFunction {

        private static final String MD5_HASH = "MD5";

        private MD5Hash() {
        }

        @Override
        public long hash(@NotNull final ByteBuffer key) {
            MessageDigest messageDigest;
            try {
                messageDigest = MessageDigest.getInstance(MD5_HASH);
                messageDigest.reset();
                final ByteBuffer duplicate = key.duplicate();
                final byte[] bytes = new byte[duplicate.remaining()];
                duplicate.get(bytes);
                messageDigest.update(bytes);
                final byte[] digest = messageDigest.digest();
                long hash = 0;
                for (int i = 0; i < 4; i++) {
                    hash <<= 8;
                    hash |= digest[i] & 0xFF;
                }
                return hash;
            } catch (NoSuchAlgorithmException e) {
                logger.error("Algorithm of hashing nof found ", e);
                throw new RuntimeException("You lost all polymers! ", e);
            }
        }
    }
}
