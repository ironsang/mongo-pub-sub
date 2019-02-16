package com.alternate.mongopubsub.websocket.services.impl;

import com.alternate.mongopubsub.messagebroker.services.MessageBroker;
import com.alternate.mongopubsub.websocket.models.Command;
import com.alternate.mongopubsub.websocket.models.Message;
import com.alternate.mongopubsub.websocket.services.ConsumerSessionHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import reactor.core.Disposable;

import java.io.IOException;
import java.util.Map;

@Service
public class WebSocketHandlerImpl extends TextWebSocketHandler {

    private final ObjectMapper objectMapper;
    private final ConsumerSessionHandler consumerSessionHandler;
    private final MessageBroker messageBroker;

    @Autowired
    public WebSocketHandlerImpl(ObjectMapper objectMapper, ConsumerSessionHandler consumerSessionHandler, MessageBroker messageBroker) {
        this.objectMapper = objectMapper;
        this.consumerSessionHandler = consumerSessionHandler;
        this.messageBroker = messageBroker;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        Message message = Message.builder()
                .withType("response")
                .withAttribute("status", "success")
                .withAttribute("scope", "publish | subscribe")
                .build();
        session.sendMessage(new TextMessage(this.objectMapper.writeValueAsString(message)));
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        this.consumerSessionHandler.removeSubscriber(session.getId());
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage textMessage) throws IOException {
        Command command = this.objectMapper.readValue(textMessage.getPayload(), Command.class);

        switch (command.getType()) {
            case PUBLISH:
                this.handlePublishMessage(session, command.getHeaders().get("topic"), command.getContent());
                break;
            case SUBSCRIBE:
                this.handleSubscribeMessage(session, command.getHeaders().get("topic"));
                break;
            case HEART_BEAT:
                this.handleHearBeatMessage(session);
                break;
            default:
                this.handleUnsupportedMessage(session);
        }
    }

    private void handlePublishMessage(WebSocketSession session, String topic, Map<String, Object> content) throws IOException {
        if (topic == null || content == null) {
            this.handleInvalidMessage(session);
            return;
        }

        this.messageBroker.publish(topic, content);
        session.sendMessage(new TextMessage(this.objectMapper.writeValueAsString(Message.builder()
                .withType("response")
                .withAttribute("status", "success")
                .build())));
    }

    private void handleSubscribeMessage(WebSocketSession session, String topic) throws IOException {
        if (topic == null) {
            this.handleInvalidMessage(session);
            return;
        }

        Disposable disposable = this.messageBroker.subscribe(topic)
                .subscribe(m -> {
                    try {
                        session.sendMessage(new TextMessage(this.objectMapper.writeValueAsString(Message.builder()
                                .withType("message")
                                .withPayload(m)
                                .build())));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
        this.consumerSessionHandler.subscribeTopic(session.getId(), topic, disposable);

        session.sendMessage(new TextMessage(this.objectMapper.writeValueAsString(Message.builder()
                .withType("response")
                .withAttribute("status", "success")
                .build())));
    }

    private void handleHearBeatMessage(WebSocketSession session) throws IOException {
        session.sendMessage(new TextMessage(this.objectMapper.writeValueAsString(Message.builder()
                .withType("heartbeat")
                .withPayload(null)
                .build())));
    }

    private void handleUnsupportedMessage(WebSocketSession session) throws IOException {
        session.sendMessage(new TextMessage(this.objectMapper.writeValueAsString(Message.builder()
                .withType("response")
                .withAttribute("status", "unsuccessful")
                .withAttribute("message", "unsupported command")
                .build())));
    }

    private void handleInvalidMessage(WebSocketSession session) throws IOException {
        session.sendMessage(new TextMessage(this.objectMapper.writeValueAsString(Message.builder()
                .withType("response")
                .withAttribute("status", "unsuccessful")
                .withAttribute("message", "invalid message body")
                .build())));
    }
}
