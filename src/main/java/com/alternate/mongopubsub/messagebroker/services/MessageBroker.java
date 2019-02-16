package com.alternate.mongopubsub.messagebroker.services;

import reactor.core.publisher.Flux;

import java.util.Map;

public interface MessageBroker {
    void publish(String topic, Map<String, Object> payload);
    Flux<Map<String, Object>> subscribe(String topic);
}
