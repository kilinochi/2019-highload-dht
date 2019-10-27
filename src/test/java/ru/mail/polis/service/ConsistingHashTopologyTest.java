package ru.mail.polis.service;

import com.google.common.base.Charsets;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.service.topology.Topology;
import ru.mail.polis.service.topology.node.ServiceNode;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Set;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ConsistingHashTopologyTest {
    private static final Logger logger = LoggerFactory.getLogger(ConsistingHashTopologyTest.class);

    private static final int KEYS_COUNT = 1000;
    private static final long VIRTUAL_NODE_COUNT = 20;
    private static Set<ServiceNode> NODES = null;
    private static ServiceNode ME = null;
    static {
        try {
            ME = new ServiceNode(new URL("http://localhost:8097"));
            NODES = Set.of(
                        new ServiceNode(new URL("http://localhost:8098")),
                        new ServiceNode(new URL("http://localhost:8099")));
        } catch (MalformedURLException e) {
            logger.error("Error while create URL ", e.getCause());
        }
    }

    private static final long EXPECTED_KEYS_PER_NODE = KEYS_COUNT / NODES.size();
    private static final int KEYS_DELTA = (int) (EXPECTED_KEYS_PER_NODE * 0.15);


    @Test
    void consistentTest() {
        final Topology<ServiceNode> topology1 = createTopology();
        final Topology<ServiceNode> topology2 = createTopology();
        for(int i = 0; i < KEYS_COUNT; i++) {
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
        for(long i = 0; i < KEYS_COUNT; i++) {
            final String key = "superKeyNumber" + i;
            final ServiceNode node = topology.primaryFor(ByteBuffer.wrap(key.getBytes(Charsets.UTF_8)));
            counter.compute(node, (u, c) -> c == null ? 1 : c + 1);
        }

        counter.forEach((node, value) -> {
            final int count = value;
            final long delta = Math.abs(EXPECTED_KEYS_PER_NODE - count);
            assertTrue(delta < KEYS_DELTA, "Node keys counter is out of range on node "
                    + node + ", delta = " + delta);
        });
    }

    private static Topology<ServiceNode> createTopology() {
        return Topology.consistingHashTopology(NODES, ME, VIRTUAL_NODE_COUNT);
    }
}
