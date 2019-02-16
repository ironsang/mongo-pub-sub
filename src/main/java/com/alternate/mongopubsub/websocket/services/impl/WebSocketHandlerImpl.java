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
        this.consumerSessionHandler.removeConsumer(session.getId());
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage textMessage) throws IOException {
        Command command = this.objectMapper.readValue(textMessage.getPayload(), Command.class);

        switch (command.getType()) {
            case PUBLISH:
                this.messageBroker.publish(command.getHeaders().get("topic"), command.getContent());
                session.sendMessage(new TextMessage(this.objectMapper.writeValueAsString(Message.builder()
                        .withType("response")
                        .withAttribute("status", "success")
                        .build())));
                break;
            case SUBSCRIBE:
                Disposable disposable = this.messageBroker.subscribe(command.getHeaders().get("topic"))
                        .subscribe(m -> {
                            try {
                                session.sendMessage(new TextMessage(this.objectMapper.writeValueAsString(Message.builder()
                                        .withType("message")
                                        .withAttribute("status", "success")
                                        .build())));
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
                this.consumerSessionHandler.addConsumer(session.getId(), disposable);
                break;
            case HEART_BEAT:
                session.sendMessage(new TextMessage(this.objectMapper.writeValueAsString(Message.builder()
                        .withType("heartbeat")
                        .withPayload(null)
                        .build())));
            default:
                session.sendMessage(new TextMessage(this.objectMapper.writeValueAsString(Message.builder()
                        .withType("response")
                        .withAttribute("status", "unsuccessful")
                        .withAttribute("message", "unsupported command")
                        .build())));
        }
    }
}
