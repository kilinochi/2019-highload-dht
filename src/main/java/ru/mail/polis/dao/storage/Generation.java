package ru.mail.polis.dao.storage;

import org.jetbrains.annotations.NotNull;
import java.nio.file.Path;

final class Generation {
    private Generation() {
    }

    static long fromPath(@NotNull final Path path) {
        return getNumericValue(path.getFileName().toString());
    }

    /**
     * Get generation by name of table.
     *
     * @param name is the name of file
     **/
    private static long getNumericValue(@NotNull final String name) {
        return Long.parseLong(name.split("_")[1].split("\\.")[0]);
    }
}
