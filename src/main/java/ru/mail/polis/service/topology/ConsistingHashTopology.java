package ru.mail.polis.service.topology;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.MessageDigest;
import java.util.*;

import static com.google.common.base.Charsets.UTF_8;

public class ConsistingHashTopology <T extends Node> implements Topology<String> {

    private static final Logger logger = LoggerFactory.getLogger(ConsistingHashTopology.class);

    private final SortedMap<Long, VirtualNode> ring = new TreeMap<>();
    private final HashFunction hashFunction;

    @NotNull
    private final String me;

    @NotNull
    private final ServiceNode[] nodes;

    public ConsistingHashTopology(
            @NotNull final Set<ServiceNode> nodes,
            @NotNull final String me,
            final long virtualNodeCount) {
        this.me = me;
        this.nodes = nodes.toArray(ServiceNode[]::new);
        this.hashFunction = new MD5Hash();
        Arrays.sort(this.nodes);
        for(ServiceNode serviceNode : nodes) {
            addNode(serviceNode, virtualNodeCount);
        }
    }

    private void addNode(ServiceNode node, long vNodeCount) {
        if(vNodeCount < 0) {
            throw new IllegalArgumentException("Illegal virtual node counts : " + vNodeCount);
        }
        int existingReplicas = getExistingReplicas(node);
        for(int i = 0; i < vNodeCount; i++) {
            VirtualNode virtualNode = new VirtualNode(node, i + existingReplicas);
            ring.put(hashFunction.hash(ByteBuffer.wrap(virtualNode.getKey().getBytes(UTF_8))), virtualNode);
        }
    }

    private int getExistingReplicas(ServiceNode node) {
        int replicas = 0;
        for(final VirtualNode virtualNode : ring.values()) {
            if(virtualNode.isVirtualNodeOf(node)) {
                replicas = replicas +1;
            }
        }
        return replicas;
    }

    public void removeNode(ServiceNode node) {
        final Iterator<Long> iterator = ring.keySet().iterator();
        while (iterator.hasNext()) {
            final Long key = iterator.next();
            final VirtualNode virtualNode = ring.get(key);
            if(virtualNode.isVirtualNodeOf(node)) {
                iterator.remove();
            }
        }
    }

    @Override
    public boolean isMe(@NotNull String node) {
        return this.me.equals(node);
    }

    @NotNull
    @Override
    public String primaryFor(@NotNull ByteBuffer key) {
        final Long hashVal = hashFunction.hash(key);
        final SortedMap<Long, VirtualNode> tailMap = ring.tailMap(hashVal);
        final Long nodeHashVal = !tailMap.isEmpty() ? tailMap.firstKey() : ring.firstKey();
        return ring.get(nodeHashVal).getPhysicalNode().getKey();
    }

    @NotNull
    @Override
    public Set<String> all() {
        return Set.of(Arrays.toString(this.nodes));
    }

    private static final class MD5Hash implements HashFunction {

        private MessageDigest messageDigest;

        private MD5Hash() {
            try {
                this.messageDigest = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                logger.error("Error :" + e.getMessage());
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
            long h = 0;
            for(int i = 0; i < 4;i++) {
                h <<= 8;
                h |= ((int) digest[i]) & 0xFF;
            }
            return h;
        }
    }
}
