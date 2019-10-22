package ru.mail.polis.service.topology.node;

import org.jetbrains.annotations.NotNull;

import java.net.URL;

public final class ServiceNode implements Node, Comparable<ServiceNode> {

    private final URL url;

    public ServiceNode(@NotNull final URL url) {
        this.url = url;
    }

    @Override
    public String key() {
        return url.toString();
    }

    @Override
    public int compareTo(@NotNull final ServiceNode serviceNode) {
        return Long.compare(this.url.getPort(), serviceNode.url.getPort());
    }
}
