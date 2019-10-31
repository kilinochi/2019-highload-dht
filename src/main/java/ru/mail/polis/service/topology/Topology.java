package ru.mail.polis.service.topology;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.service.topology.node.Node;
import ru.mail.polis.service.topology.node.ServiceNode;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.util.Set;

@ThreadSafe
public interface Topology<T extends Node> {
    boolean isMe(@NotNull final T node);

    @NotNull
    T primaryFor(@NotNull final ByteBuffer key);

    @NotNull
    Set<T> all();

    int size();

    @NotNull
    T[] replicas(final int count, @NotNull final ByteBuffer key);

    @NotNull
    static Topology<ServiceNode> basic(@NotNull Set<ServiceNode> serviceNodes,
                                       @NotNull ServiceNode me) {
        return new BasicTopology(serviceNodes, me);
    }

    @NotNull
    static Topology<ServiceNode> consistentHashTopology(@NotNull Set<ServiceNode> serviceNodes,
                                                        @NotNull ServiceNode me,
                                                        final long virtualNodeCount) {
        return new ConsistentHashTopology(serviceNodes, me, virtualNodeCount);
    }
}
