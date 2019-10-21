package ru.mail.polis.service.topology;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Set;
import java.security.MessageDigest;

public class ConsistingHashTopology <T extends Node> implements Topology<String> {

    private static final Logger logger = LoggerFactory.getLogger(ConsistingHashTopology.class);

    @NotNull
    private final String me;

    @NotNull
    private final String[] nodes;

    public ConsistingHashTopology(
            @NotNull final Set<String> nodes,
            @NotNull final String me) {
        this.me = me;
        this.nodes = new String[nodes.size()];
        nodes.toArray(this.nodes);
        Arrays.sort(this.nodes);
    }


    @Override
    public boolean isMe(@NotNull String node) {
        return this.me.equals(node);
    }

    @NotNull
    @Override
    public String primaryFor(@NotNull ByteBuffer key) {
        return null;
    }

    @NotNull
    @Override
    public Set<String> all() {
        return Set.of(this.nodes);
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
        public long hash(String key) {
            messageDigest.reset();
            messageDigest.update(key.getBytes());
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
