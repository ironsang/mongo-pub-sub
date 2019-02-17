package com.alternate.websocket.services.impl;

import com.alternate.messagebroker.services.MessageBroker;
import com.alternate.websocket.models.Message;
import com.alternate.websocket.models.MessageType;
import com.alternate.websocket.services.ConsumerSessionHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketHandlerImpl.class);

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
        LOGGER.info("client: {} connected", session.getId());
        Message message = Message.builder()
                .withType(MessageType.RESPONSE)
                .withHeaders(null)
                .withContentAttribute("status", "success")
                .withContentAttribute("scope", "publish | subscribe")
                .build();
        this.sendMessage(session, message);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        LOGGER.info("client: {} disconnected", session.getId());
        this.consumerSessionHandler.removeSubscriber(session.getId());
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage textMessage) throws IOException {
        LOGGER.info("message: {} received from client: {}", textMessage.getPayload(), session.getId());
        Message message = this.objectMapper.readValue(textMessage.getPayload(), Message.class);

        if (message.getType() != MessageType.HEART_BEAT && message.getType() != MessageType.COMMAND) {
            this.handleUnsupportedMessage(session);
            return;
        }

        if (message.getType() == MessageType.HEART_BEAT) {
            this.handleHearBeatMessage(session);
            return;
        }

        if (message.getHeaders() == null) {
            this.handleInvalidMessage(session);
            return;
        }

        String command = message.getHeaders().get("command");

        if (command == null) {
            this.handleInvalidMessage(session);
            return;
        }

        switch (command) {
            case "PUBLISH":
                this.handlePublishMessage(session, message.getHeaders().get("topic"), message.getContent());
                break;
            case "SUBSCRIBE":
                this.handleSubscribeMessage(session, message.getHeaders().get("topic"), message.getContent().get("filter"));
                break;
            default:
                this.handleUnsupportedMessage(session);
                break;
        }
    }

    private void handlePublishMessage(WebSocketSession session, String topic, Map<String, Object> content) throws IOException {
        if (topic == null || content == null) {
            this.handleInvalidMessage(session);
            return;
        }

        this.messageBroker.publish(topic, content);
        Message message = Message.builder()
                .withType(MessageType.RESPONSE)
                .withHeaders(null)
                .withContentAttribute("status", "success")
                .build();
        this.sendMessage(session, message);
    }

    private void handleSubscribeMessage(WebSocketSession session, String topic, Object object) throws IOException {
        if (topic == null) {
            this.handleInvalidMessage(session);
            return;
        }

        Map<String, Object> filter = (object != null) ? (Map<String, Object>) object : null;

        Disposable disposable = this.messageBroker.subscribe(topic, filter)
                .subscribe(m -> {
                    try {
                        Message message = Message.builder()
                                .withType(MessageType.MESSAGE)
                                .withHeaders(null)
                                .withContent(m)
                                .build();
                        this.sendMessage(session, message);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
        this.consumerSessionHandler.subscribeTopic(session.getId(), topic, disposable);

        Message message = Message.builder()
                .withType(MessageType.RESPONSE)
                .withHeaders(null)
                .withContentAttribute("status", "success")
                .build();
        this.sendMessage(session, message);
    }

    private void handleHearBeatMessage(WebSocketSession session) throws IOException {
        Message message = Message.builder()
                .withType(MessageType.HEART_BEAT)
                .withHeaders(null)
                .withContent(null)
                .build();
        this.sendMessage(session, message);
    }

    private void handleUnsupportedMessage(WebSocketSession session) throws IOException {
        Message message = Message.builder()
                .withType(MessageType.RESPONSE)
                .withHeaders(null)
                .withContentAttribute("status", "unsuccessful")
                .withContentAttribute("message", "unsupported command")
                .build();
        this.sendMessage(session, message);
    }

    private void handleInvalidMessage(WebSocketSession session) throws IOException {
        Message message = Message.builder()
                .withType(MessageType.RESPONSE)
                .withHeaders(null)
                .withContentAttribute("status", "unsuccessful")
                .withContentAttribute("message", "invalid message body")
                .build();
        this.sendMessage(session, message);
    }

    private void sendMessage(WebSocketSession session, Message message) throws IOException {
        String messageString = this.objectMapper.writeValueAsString(message);
        session.sendMessage(new TextMessage(messageString));
        LOGGER.info("message: {} sent to client: {}", messageString, session.getId());
    }
}
