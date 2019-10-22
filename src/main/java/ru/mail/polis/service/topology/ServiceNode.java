package ru.mail.polis.service.topology;

import org.jetbrains.annotations.NotNull;

import java.net.URL;

public final class ServiceNode implements Node, Comparable<ServiceNode> {

    private final URL url;

    public ServiceNode(final URL url) {
        this.url = url;
    }

    @Override
    public String url() {
        return url.toString();
    }

    @Override
    public int compareTo(@NotNull ServiceNode serviceNode) {
        return Integer.compare(this.url.getPort(), serviceNode.url.getPort());
    }
}
