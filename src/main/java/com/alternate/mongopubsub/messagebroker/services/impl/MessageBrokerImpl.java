package com.alternate.mongopubsub.messagebroker.services.impl;

import com.alternate.mongopubsub.messagebroker.models.MessageWrapper;
import com.alternate.mongopubsub.messagebroker.services.MessageBroker;
import org.springframework.stereotype.Service;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.Map;

@Service
public class MessageBrokerImpl implements MessageBroker {

    private final Flux<MessageWrapper> messagFlux;
    private final FluxSink<MessageWrapper> messagFluxSink;

    public MessageBrokerImpl() {
        final DirectProcessor<MessageWrapper> directProcessor = DirectProcessor.create();
        this.messagFlux = directProcessor.onBackpressureBuffer();
        this.messagFluxSink = directProcessor.sink();
    }

    @Override
    public void publish(String topic, Map<String, Object> payload) {
        MessageWrapper messageWrapper = MessageWrapper.builder()
                .withTopic(topic)
                .withPayload(payload)
                .build();
        this.messagFluxSink.next(messageWrapper);
    }

    @Override
    public Flux<Map<String, Object>> subscribe(String topic) {
        return this.messagFlux
                .filter(messageWrapper -> messageWrapper.getTopic().equals(topic))
                .map(MessageWrapper::getPayload);
    }
}
