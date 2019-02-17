package com.alternate.websocket.models;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.HashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Message {
    private MessageType type;
    private Map<String, String> headers;
    private Map<String, Object> content;

    private Message() {
        // for jackson databind
    }

    private Message(MessageBuilder builder) {
        this.type = builder.type;
        this.headers = builder.headers;
        this.content = builder.content;
    }

    public MessageType getType() {
        return type;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public Map<String, Object> getContent() {
        return content;
    }

    public static MessageBuilder builder() {
        return new MessageBuilder();
    }

    public static class MessageBuilder {
        private MessageType type;
        private Map<String, String> headers = new HashMap<>();
        private Map<String, Object> content = new HashMap<>();

        public MessageBuilder withType(MessageType type) {
            this.type = type;
            return this;
        }

        public MessageBuilder withHeaders(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }

        public MessageBuilder withContent(Map<String, Object> content) {
            this.content = content;
            return this;
        }

        public MessageBuilder withHeaderAttribute(String key, String value) {
            this.headers.put(key, value);
            return this;
        }

        public MessageBuilder withContentAttribute(String key, Object value) {
            this.content.put(key, value);
            return this;
        }

        public Message build() {
            return new Message(this);
        }
    }
}
