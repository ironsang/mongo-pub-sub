package com.alternate.messagebroker.services.impl;

import com.alternate.common.util.Executors2;
import com.alternate.messagebroker.models.MessageWrapper;
import com.alternate.messagebroker.services.MessageBroker;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

@Service
public class MessageBrokerImpl implements MessageBroker {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageBrokerImpl.class);

    private ExecutorService executor;
    private Flux<MessageWrapper> messagFlux;
    private FluxSink<MessageWrapper> messagFluxSink;

    private final MongoDatabase mongoDatabase;

    @Autowired
    public MessageBrokerImpl(MongoDatabase mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
        this.init();
    }

    @Override
    public void publish(String topic, Map<String, Object> payload) {
        this.executor.submit(() -> this.persistDocument(topic, payload));
        LOGGER.info("client submitted message to topic: {}", topic);
    }

    @Override
    public Flux<Map<String, Object>> subscribe(String topic, Map<String, Object> filter) {
        Flux<MessageWrapper> messageWrapperFlux = this.messagFlux
                .filter(messageWrapper -> messageWrapper.getTopic().equals(topic));

        for (String key : filter.keySet()) {
            Object value = filter.get(key);
            messageWrapperFlux = messageWrapperFlux
                    .filter(m -> m.getPayload().get(key).equals(value));
        }

        LOGGER.info("client subscribed to topic: {}", topic);
        return messageWrapperFlux
                .map(MessageWrapper::getPayload);
    }

    private void init() {
        this.executor = Executors.newSingleThreadExecutor();

        final DirectProcessor<MessageWrapper> directProcessor = DirectProcessor.create();
        this.messagFlux = directProcessor.onBackpressureBuffer();
        this.messagFluxSink = directProcessor.sink();

        this.initChangeStreamListener();
    }

    private void initChangeStreamListener() {
        List<Bson> pipeline = Collections.singletonList(Aggregates.match(
                Filters.in("operationType", Arrays.asList("insert", "update", "replace"))
        ));

        Executors2.newRetrySingleThreadExecutor(5).submit(() -> {
            LOGGER.info("mongo change stream listener started");
            this.mongoDatabase
                    .watch(pipeline)
                    .fullDocument(FullDocument.UPDATE_LOOKUP)
                    .forEach((Consumer<? super ChangeStreamDocument<Document>>) this::processDocument);
        });
    }

    private void processDocument(ChangeStreamDocument<Document> document) {
        String topic = document.getNamespace() != null ? document.getNamespace().getCollectionName() : "null";
        Map<String, Object> payload = document.getFullDocument();
        String id = document.getDocumentKey().get("_id").asObjectId().getValue().toString();
        payload.put("_id", id);

        this.messagFluxSink.next(MessageWrapper.builder()
                .withTopic(topic)
                .withPayload(payload)
                .build());
        LOGGER.info("message published to topic: {}", id, topic);
    }

    private void persistDocument(String topic, Map<String, Object> payload) {
        MongoCollection<Document> collection = this.mongoDatabase.getCollection(topic);

        String id = (String) payload.get("_id");
        payload.remove("_id");

        Document document = new Document();
        payload.forEach(document::append);

        if (id == null) {
            collection.insertOne(document);
        } else {
            collection.replaceOne(Filters.eq("_id", new ObjectId(id)), document, new ReplaceOptions().upsert(true));
        }
    }
}
