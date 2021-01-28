package io.managed.services.test.client.kafka;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.apache.kafka.common.KafkaFuture;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class KafkaUtils {

    static public Map<String, String> configs(String bootstrapHost, String clientID, String clientSecret) {
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", bootstrapHost);
        config.put("sasl.mechanism", "PLAIN");
        config.put("security.protocol", "SASL_SSL");
        config.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", clientID, clientSecret));
        return config;
    }

    static public <T> CompletionStage<T> toCompletionStage(KafkaFuture<T> future) {
        CompletableFuture<T> completable = new CompletableFuture<>();
        future.whenComplete((r, e) -> {
            if (e == null) {
                completable.complete(r);
            } else {
                completable.completeExceptionally(e);
            }
        });
        return completable;
    }

    static public KafkaProducer<String, String> createProducer(
            Vertx vertx, String bootstrapHost, String clientID, String clientSecret) {

        Map<String, String> config = KafkaUtils.configs(bootstrapHost, clientID, clientSecret);
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("acks", "all");

        return KafkaProducer.create(vertx, config);
    }

    public static KafkaConsumer<String, String> createConsumer(
            Vertx vertx, String bootstrapHost, String clientID, String clientSecret) {

        Map<String, String> config = KafkaUtils.configs(bootstrapHost, clientID, clientSecret);
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("group.id", "test-group");
        config.put("auto.offset.reset", "earliest");
        config.put("enable.auto.commit", "false");

        return KafkaConsumer.create(vertx, config);
    }
}
