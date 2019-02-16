package com.alternate.mongopubsub.websocket.services;

import reactor.core.Disposable;

public interface ConsumerSessionHandler {
    void subscribeTopic(String id, String topic, Disposable disposable);

    void unsubscribeTopic(String id, String topic);

    void unsubscribeAllTopics(String id);

    void removeSubscriber(String id);
}
