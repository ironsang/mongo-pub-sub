package com.alternate.mongopubsub.messagebroker.services.impl;

import com.alternate.mongopubsub.messagebroker.models.MessageWrapper;
import com.alternate.mongopubsub.messagebroker.services.MessageBroker;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class MessageBrokerImpl implements MessageBroker {
    private ExecutorService executor;
    private ScheduledExecutorService scheduler;
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
    }

    @Override
    public Flux<Map<String, Object>> subscribe(String topic, Map<String, Object> filter) {
        Flux<MessageWrapper> messageWrapperFlux = this.messagFlux
                .filter(messageWrapper -> messageWrapper.getTopic().equals(topic));

        for (String key: filter.keySet()) {
            Object value = filter.get(key);
            messageWrapperFlux = messageWrapperFlux.filter(m -> m.getPayload().get(key).equals(value));
        }

        return messageWrapperFlux
                .map(MessageWrapper::getPayload);
    }

    private void init() {
        this.executor = Executors.newSingleThreadExecutor();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();

        final DirectProcessor<MessageWrapper> directProcessor = DirectProcessor.create();
        this.messagFlux = directProcessor.onBackpressureBuffer();
        this.messagFluxSink = directProcessor.sink();

        this.initChangeStreamListener();
    }

    private void initChangeStreamListener() {
        List<Bson> pipeline = Collections.singletonList(Aggregates.match(
                Filters.in("operationType", Arrays.asList("insert", "update", "replace"))
        ));

        MongoCursor<ChangeStreamDocument<Document>> cursor = this.mongoDatabase
                .watch(pipeline)
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                .iterator();

        this.scheduler.scheduleAtFixedRate(() -> {
            while (cursor.hasNext()) {
                ChangeStreamDocument<Document> document = cursor.next();
                this.processDocument(document);
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    private void processDocument(ChangeStreamDocument<Document> document) {
        String topic = document.getNamespace() != null ? document.getNamespace().getCollectionName() : "null";
        Map<String, Object> payload = document.getFullDocument();
        payload.put("_id", document.getDocumentKey().get("_id").asObjectId().getValue().toString());

        this.messagFluxSink.next(MessageWrapper.builder()
                .withTopic(topic)
                .withPayload(payload)
                .build());
    }

    private void persistDocument(String topic, Map<String, Object> payload) {
        MongoCollection<Document> collection = this.mongoDatabase.getCollection(topic);

        Document document = new Document();
        payload.remove("_id");
        payload.forEach(document::append);

        collection.updateOne(new Document(), new BasicDBObject("$set", document), new UpdateOptions().upsert(true));
    }
}
