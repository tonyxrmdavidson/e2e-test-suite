package io.managed.services.test;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;

import java.util.HashMap;
import java.util.Map;

public class KafkaUtils {

    static public KafkaProducer<String, String> createProducer(
            Vertx vertx, String bootstrapHost, String clientID, String clientSecret) {

        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", bootstrapHost);
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("sasl.mechanism", "PLAIN");
        config.put("security.protocol", "SASL_SSL");
        config.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", clientID, clientSecret));

        return KafkaProducer.create(vertx, config);
    }
}
