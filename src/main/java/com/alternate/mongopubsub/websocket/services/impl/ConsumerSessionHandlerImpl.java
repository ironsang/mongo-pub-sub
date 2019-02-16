package com.alternate.mongopubsub.websocket.services.impl;

import com.alternate.mongopubsub.websocket.services.ConsumerSessionHandler;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ConsumerSessionHandlerImpl implements ConsumerSessionHandler {
    private Map<String, Map<String, Disposable>> consumers = new ConcurrentHashMap<>();

    @Override
    public void subscribeTopic(String id, String topic, Disposable disposable) {
        this.unsubscribeTopic(id, topic);

        Map<String, Disposable> disposableMap = this.consumers.computeIfAbsent(id, k -> new ConcurrentHashMap<>());
        disposableMap.put(topic, disposable);
    }

    @Override
    public void unsubscribeTopic(String id, String topic) {
        Map<String, Disposable> disposableMap = this.consumers.get(id);

        if (disposableMap == null) {
            return;
        }

        Disposable disposable = disposableMap.get(topic);

        if (disposable == null) {
            return;
        }

        disposable.dispose();
        disposableMap.remove(topic);
    }

    @Override
    public void unsubscribeAllTopics(String id) {
        Map<String, Disposable> disposableMap = this.consumers.get(id);

        if (disposableMap == null) {
            return;
        }

        disposableMap.values().forEach(Disposable::dispose);
        disposableMap.clear();
    }

    @Override
    public void removeSubscriber(String id) {
        this.unsubscribeAllTopics(id);
        this.consumers.remove(id);
    }
}
