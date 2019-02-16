package com.alternate.mongopubsub.messagebroker.configs;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BeanConfig {

    @Value("${mongo.host}")
    private String mongoHost;

    @Value("${mongo.port}")
    private int mongoPort;

    @Value("${mongo.db}")
    private String mongoDb;

    @Bean
    public MongoClient mongoClient() {
        return new MongoClient(this.mongoHost, this.mongoPort);
    }

    @Bean
    public MongoDatabase mongoDatabase(MongoClient mongoClient) {
        return mongoClient.getDatabase(this.mongoDb);
    }

}
