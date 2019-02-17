package com.alternate.websocket.services.impl;

import com.alternate.websocket.services.ConsumerSessionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ConsumerSessionHandlerImpl implements ConsumerSessionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerSessionHandlerImpl.class);

    private Map<String, Map<String, Disposable>> consumers = new ConcurrentHashMap<>();

    @Override
    public void subscribeTopic(String id, String topic, Disposable disposable) {
        this.unsubscribeTopic(id, topic);

        Map<String, Disposable> disposableMap = this.consumers.computeIfAbsent(id, k -> new ConcurrentHashMap<>());
        disposableMap.put(topic, disposable);
        LOGGER.info("client: {} session saved for: {}", id, topic);
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
        LOGGER.info("client: {} session removed for: {}", id, topic);
    }

    @Override
    public void unsubscribeAllTopics(String id) {
        Map<String, Disposable> disposableMap = this.consumers.get(id);

        if (disposableMap == null) {
            return;
        }

        disposableMap.values().forEach(Disposable::dispose);
        disposableMap.clear();
        LOGGER.info("client: {} unsubscribed all sessions", id);
    }

    @Override
    public void removeSubscriber(String id) {
        this.unsubscribeAllTopics(id);
        this.consumers.remove(id);
        LOGGER.info("client: {} removed", id);
    }
}
