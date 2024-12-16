package com.websocket;

import okhttp3.*;
import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class BitqueryWebsocketClient {

    private static final String BEARER_TOKEN = "ory_at_...";
    private static final String WS_URL = "wss://streaming.bitquery.io/graphql";
    private static OkHttpClient client;
    private static Request request;
    private static final String LOG_FILE = "bitquery_logs.txt";
    private static BufferedWriter logWriter;

    public static void main(String[] args) {
        try {
            logWriter = new BufferedWriter(new FileWriter(LOG_FILE, true));
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        client = new OkHttpClient.Builder()
                .readTimeout(0, TimeUnit.MILLISECONDS)
                .build();

        request = new Request.Builder()
                .url(WS_URL)
                .header("Sec-WebSocket-Protocol", "graphql-ws")
                .addHeader("Authorization", "Bearer " + BEARER_TOKEN)
                .build();

        connectWebSocket();
    }

    private static void connectWebSocket() {
        client.newWebSocket(request, new WebSocketListener() {

            @Override
            public void onOpen(WebSocket webSocket, Response response) {
                log("Opened connection");
                String initMessage = "{ \"type\": \"connection_init\" }";
                webSocket.send(initMessage);

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                String payload = createSubscriptionPayload("1", "start");
                webSocket.send(payload);
            }

            @Override
            public void onMessage(WebSocket webSocket, String text) {

                JSONObject jsonObject = new JSONObject(text);

                if (!"ka".equals(jsonObject.optString("type"))) {
                    log("Update received from WebSocket.");
                    log("Received: " + text);
                }
            }

            @Override
            public void onClosed(WebSocket webSocket, int code, String reason) {
                log("Closed: " + reason);
            }

            @Override
            public void onFailure(WebSocket webSocket, Throwable t, Response response) {
                log("WebSocket Failure: " + t.getMessage());
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
                connectWebSocket();
            }
        });
    }

    private static String createSubscriptionPayload(String id, String type) {
        JSONObject payloadObject = new JSONObject()
                .put("id", id)
                .put("type", type)
                .put("payload", new JSONObject()
                        .put("query", "subscription MyQuery { " +
                                "  EVM(network: eth) { " +
                                "    Blocks { " +
                                "      Block { " +
                                "        Number " +
                                "        Time " +
                                "      } " +
                                "    } " +
                                "  } " +
                                "}")
                        .put("variables", new JSONObject()));

        return payloadObject.toString();
    }

    private static void log(String message) {
        System.out.println(message); // Print console
        try {
            logWriter.write(message);   // Write file
            logWriter.newLine();
            logWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}