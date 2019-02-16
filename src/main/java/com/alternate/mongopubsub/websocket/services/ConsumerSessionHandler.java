package com.alternate.mongopubsub.websocket.services;

import reactor.core.Disposable;

public interface ConsumerSessionHandler {
    void addConsumer(String id, Disposable disposable);

    void removeConsumer(String id);
}
