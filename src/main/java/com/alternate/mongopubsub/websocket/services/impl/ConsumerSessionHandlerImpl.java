package com.alternate.mongopubsub.websocket.services.impl;

import com.alternate.mongopubsub.websocket.services.ConsumerSessionHandler;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ConsumerSessionHandlerImpl implements ConsumerSessionHandler {
    private Map<String, Disposable> consumers = new ConcurrentHashMap<>();

    @Override
    public void addConsumer(String id, Disposable disposable) {
        consumers.put(id, disposable);
    }

    @Override
    public void removeConsumer(String id) {
        Disposable disposable = this.consumers.get(id);
        disposable.dispose();
    }
}
