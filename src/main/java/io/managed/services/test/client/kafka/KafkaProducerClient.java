package io.managed.services.test.client.kafka;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaProducerClient {
    private static final Logger LOGGER = LogManager.getLogger(KafkaProducerClient.class);
    private final Vertx vertx;
    private final KafkaProducer<String, String> producer;

    public KafkaProducerClient(Vertx vertx, String bootstrapHost, String clientID, String clientSecret, boolean oauth) {
        this.vertx = vertx;

        LOGGER.info("initialize kafka producer; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        if (oauth) {
            producer = createProducer(vertx, bootstrapHost, clientID, clientSecret);
        } else {
            producer = createProducerWithPlain(vertx, bootstrapHost, clientID, clientSecret);
        }
    }

    public Future<List<RecordMetadata>> sendAsync(String topicName, String... messages) {
        return sendAsync(topicName, Arrays.asList(messages));
    }

    public Future<List<RecordMetadata>> sendAsync(String topicName, List<String> messages) {

        List<Future> sent = messages.stream()
                .map(message -> producer.send(KafkaProducerRecord.create(topicName, message)))
                .collect(Collectors.toList());

        return CompositeFuture.all(sent)
                .onComplete(__ -> LOGGER.info("successfully sent {} messages to topic: {}", messages.size(), topicName))
                .map(c -> c.list());
    }

    static public KafkaProducer<String, String> createProducerWithPlain(
            Vertx vertx, String bootstrapHost, String clientID, String clientSecret) {

        Map<String, String> config = KafkaUtils.plainConfigs(bootstrapHost, clientID, clientSecret);
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("acks", "all");

        return KafkaProducer.create(vertx, config);
    }

    static public KafkaProducer<String, String> createProducer(
        Vertx vertx, String bootstrapHost, String clientID, String clientSecret) {
        Map<String, String> config = KafkaUtils.configs(bootstrapHost, clientID, clientSecret);


        //Standard consumer config
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("acks", "all");

        return KafkaProducer.create(vertx, config);
    }

    public Future<Void> close() {
        if (vertx != null && producer != null) {
            return producer.close()
                    .onSuccess(v -> LOGGER.info("KafkaProducerClient closed"))
                    .onFailure(c -> LOGGER.error("failed to close KafkaProducerClient", c));
        }
        return Future.succeededFuture(null);
    }
}
