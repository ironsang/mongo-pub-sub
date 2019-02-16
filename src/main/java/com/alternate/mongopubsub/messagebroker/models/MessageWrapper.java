package com.alternate.mongopubsub.messagebroker.models;

import java.util.Map;

public class MessageWrapper {
    private final String topic;
    private final Map<String, Object> payload;

    private MessageWrapper(MessageWrapperBuilder builder) {
        this.topic = builder.topic;
        this.payload = builder.payload;
    }

    public String getTopic() {
        return topic;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public static MessageWrapperBuilder builder() {
        return new MessageWrapperBuilder();
    }

    public static class MessageWrapperBuilder {
        private String topic;
        private Map<String, Object> payload;

        public MessageWrapperBuilder withTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public MessageWrapperBuilder withPayload(Map<String, Object> payload) {
            this.payload = payload;
            return this;
        }

        public MessageWrapper build() {
            return new MessageWrapper(this);
        }
    }
}
