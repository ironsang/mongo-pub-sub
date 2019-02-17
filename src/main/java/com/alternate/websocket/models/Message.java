package com.alternate.websocket.models;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.HashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Message {
    private final String type;
    private final Map<String, Object> payload;

    private Message(MessageBuilder builder) {
        this.type = builder.type;
        this.payload = builder.payload;
    }

    public String getType() {
        return type;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public static MessageBuilder builder() {
        return new MessageBuilder();
    }

    public static class MessageBuilder {
        private String type;
        private Map<String, Object> payload = new HashMap<>();

        public MessageBuilder withType(String type) {
            this.type = type;
            return this;
        }

        public MessageBuilder withPayload(Map<String, Object> payload) {
            this.payload = payload;
            return this;
        }

        public MessageBuilder withAttribute(String key, Object value) {
            this.payload.put(key, value);
            return this;
        }

        public Message build() {
            return new Message(this);
        }
    }
}
