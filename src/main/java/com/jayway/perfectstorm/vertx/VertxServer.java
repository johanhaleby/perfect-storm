package com.jayway.perfectstorm.vertx;

import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.ServerWebSocket;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class VertxServer {

    private List<ServerWebSocket> connections = new CopyOnWriteArrayList<ServerWebSocket>();
    private Vertx vertx;

    public void start() {
        vertx = Vertx.newVertx();
        HttpServer server = vertx.createHttpServer();

        server.websocketHandler(new Handler<ServerWebSocket>() {
            public void handle(final ServerWebSocket ws) {
                // A WebSocket has connected!
                connections.add(ws);
                if (ws.path.equals("/subscribe")) {
                    ws.dataHandler(new Handler<Buffer>() {
                        public void handle(Buffer data) {
                            final String message = data.toString();
                            broadcast(message);
                        }
                    });
                    ws.endHandler(new Handler<Void>() {
                        @Override
                        public void handle(Void aVoid) {
                            connections.remove(ws);
                        }
                    });
                } else {
                    ws.reject();
                }
            }
        }).requestHandler(new Handler<HttpServerRequest>() {
            public void handle(HttpServerRequest req) {
                if (req.path.equals("/")) req.response.sendFile("src/html/ws.html"); // Serve the html
            }
        }).listen(8080);
    }

    public void stop() {
        vertx.stop();
    }

    private void broadcast(String message) {
        for (ServerWebSocket connection : connections) {
            connection.writeTextFrame(message);
        }
    }
}
