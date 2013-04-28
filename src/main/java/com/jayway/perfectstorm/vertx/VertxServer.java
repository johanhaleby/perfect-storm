package com.jayway.perfectstorm.vertx;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.ServerWebSocket;
import org.vertx.java.core.json.JsonObject;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class VertxServer {

    private final ScheduledExecutorService executorService;
    private final HazelcastInstance hazelcast;

    public VertxServer() {
        hazelcast = Hazelcast.newHazelcastInstance();
        BlockingQueue<Map<String, Object>> tpsQueue = hazelcast.getQueue("tweets-per-second");
        BlockingQueue<Map<String, Object>> countryAndTweetFrequencyQueue = hazelcast.getQueue("country-frequency");
        executorService = Executors.newScheduledThreadPool(2);
        executorService.scheduleAtFixedRate(new QueueBroadcaster(tpsQueue), 200, 50, MILLISECONDS);
        executorService.scheduleAtFixedRate(new QueueBroadcaster(countryAndTweetFrequencyQueue), 200, 50, MILLISECONDS);
    }

    private List<ServerWebSocket> connections = new CopyOnWriteArrayList<>();
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
                if (req.path.equals("/smoothie.js")) req.response.sendFile("src/js/smoothie.js"); // Serve the html
            }
        }).listen(8080);
    }

    public void stop() {
        vertx.stop();
        hazelcast.getLifecycleService().shutdown();
        executorService.shutdown();
        try {
            executorService.awaitTermination(1, SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void broadcast(String message) {
        for (ServerWebSocket connection : connections) {
            connection.writeTextFrame(message);
        }
    }

    private class QueueBroadcaster implements Runnable {
        private final BlockingQueue<Map<String, Object>> inputQueue;

        public QueueBroadcaster(BlockingQueue<Map<String, Object>> inputQueue) {
            this.inputQueue = inputQueue;
        }

        @Override
        public void run() {
            final Map<String, Object> object = inputQueue.poll();
            if (object == null) {
                return;
            }

            final String json = new JsonObject(object).encode();
            broadcast(json);
        }
    }
}
