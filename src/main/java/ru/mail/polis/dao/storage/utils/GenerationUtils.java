package ru.mail.polis.dao.storage.utils;

import org.jetbrains.annotations.NotNull;
import java.nio.file.Path;

public final class GenerationUtils {
    private GenerationUtils() {
    }

    public static long fromPath(@NotNull final Path path) {
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
