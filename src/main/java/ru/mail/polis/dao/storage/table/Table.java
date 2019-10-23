package ru.mail.polis.dao.storage.table;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.storage.cell.Cell;

import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Table {
    long size();

    @NotNull
    Iterator<Cell> iterator(@NotNull final ByteBuffer from);

    void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value);

    void remove(@NotNull ByteBuffer key);

    long generation();
}
