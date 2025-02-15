package ru.mail.polis.service;

import com.google.common.base.Charsets;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.service.topology.Topology;
import ru.mail.polis.service.topology.node.ServiceNode;
import ru.mail.polis.utils.BytesUtils;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ConsistentHashTopologyTest {
    private static final Logger logger = LoggerFactory.getLogger(ConsistentHashTopologyTest.class);

    private static final int KEYS_COUNT = 1000;
    private static final long VIRTUAL_NODE_COUNT = 65;
    private static Set<ServiceNode> NODES;
    private static ServiceNode ME;
    private static long EXPECTED_KEYS_PER_NODE;
    private static int KEYS_DELTA;
    private static int replicasCount;

    @BeforeAll
    static void setUp() {
        try {
            ME = new ServiceNode(new URL("http://localhost:8097"));
            NODES = Set.of(
                    new ServiceNode(new URL("http://localhost:8098")),
                    new ServiceNode(new URL("http://localhost:8099")),
                    new ServiceNode(new URL("http://localhost:8100")),
                    new ServiceNode(new URL("http://localhost:8101")),
                    new ServiceNode(new URL("http://localhost:8102")),
                    new ServiceNode(new URL("http://localhost:8103")),
                    new ServiceNode(new URL("http://localhost:8104")));
            EXPECTED_KEYS_PER_NODE = KEYS_COUNT / NODES.size();
            KEYS_DELTA = (int) (EXPECTED_KEYS_PER_NODE * 0.15);
            replicasCount = NODES.size() - 3;
        } catch (MalformedURLException e) {
            logger.error("Error while create URL ", e.getCause());
        }
    }

    @Test
    void consistentTest() {
        final Topology<ServiceNode> topology1 = createTopology();
        final Topology<ServiceNode> topology2 = createTopology();
        for (int i = 0; i < KEYS_COUNT; i++) {
            final ByteBuffer key = ByteBuffer.wrap(("key" + i).getBytes(Charsets.UTF_8));
            final ServiceNode node1 = topology1.primaryFor(key.duplicate());
            final ServiceNode node2 = topology2.primaryFor(key.duplicate());
            assertEquals(node1.key(), node2.key());
        }
    }

    @Test
    void testUniform() {
        final Topology<ServiceNode> topology = createTopology();
        final Map<ServiceNode, Integer> counter = new HashMap<>();
        for (long i = 0; i < KEYS_COUNT; i++) {
            final String key = "superKeyNumber" + i;
            final ServiceNode node = topology.primaryFor(ByteBuffer.wrap(key.getBytes(Charsets.UTF_8)));
            counter.compute(node, (u, c) -> c == null ? 1 : c + 1);
        }

        counter.forEach((node, value) -> {
            final int count = value;
            final long delta = Math.abs(EXPECTED_KEYS_PER_NODE - count);
            assertTrue(delta < KEYS_DELTA, "Node keys counter is out of range on node "
                    + node + ", delta = " + delta + " expected keys delta : " + KEYS_DELTA);
        });
    }

    @Test
    void replicasTest() {
        final Topology<ServiceNode> topology = createTopology();
        for (long i = 0; i < KEYS_COUNT; i++) {
            final String key = "superPuperKey" + i;
            final List<ServiceNode> nodes = topology.replicas(replicasCount, BytesUtils.keyByteBuffer(key));
            assertEquals(replicasCount, nodes.size());
            //delete duplicates and check size
            long size = nodes.stream().distinct().count();
            assertEquals(replicasCount, size);
        }
    }

    private static Topology<ServiceNode> createTopology() {
        return Topology.consistentHashTopology(NODES, ME, VIRTUAL_NODE_COUNT);
    }
}
