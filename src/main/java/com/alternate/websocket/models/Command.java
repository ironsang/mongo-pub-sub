package com.alternate.websocket.models;

import java.util.Map;

public class Command {
    private CommandType type;
    private Map<String, String> headers;
    private Map<String, Object> content;

    private Command() {
        // for jackson databind
    }

    public Command(CommandType type, Map<String, String> headers, Map<String, Object> content) {
        this.type = type;
        this.headers = headers;
        this.content = content;
    }

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
