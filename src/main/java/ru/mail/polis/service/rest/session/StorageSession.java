package ru.mail.polis.service.rest.session;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.net.Socket;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.util.Iterator;

public class StorageSession extends HttpSession {
    public StorageSession(Socket socket, HttpServer server) {
        super(socket, server);
    }

    public void stream(@NotNull final Iterator <Record> records) {

    }
}
