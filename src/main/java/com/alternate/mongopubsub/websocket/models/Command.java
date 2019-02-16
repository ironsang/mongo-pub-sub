package com.alternate.mongopubsub.websocket.models;

import java.util.Map;

public class Command {
    private CommandType type;
    private Map<String, String> headers;
    private Map<String, Object> content;

    public CommandType getType() {
        return type;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public Map<String, Object> getContent() {
        return content;
    }
}
