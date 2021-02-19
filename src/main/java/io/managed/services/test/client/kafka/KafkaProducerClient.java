package io.managed.services.test.client.kafka;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.fail;

public class KafkaProducerClient {
    private static final Logger LOGGER = LogManager.getLogger(KafkaProducerClient.class);
    private Vertx vertx;
    private String topic;
    private String bootstrapHost;
    private String clientID;
    private String clientSecret;
    private KafkaProducer<String, String> producer;

    public KafkaProducerClient(Vertx vertx, String topicName, String bootstrapHost, String clientID, String clientSecret) {
        this.vertx = vertx;
        this.topic = topicName;
        this.bootstrapHost = bootstrapHost;
        this.clientID = clientID;
        this.clientSecret = clientSecret;
    }

    public void sendAsync(String... messages) {
        sendAsync(Arrays.asList(messages));
    }

    public List<Future<RecordMetadata>> sendAsync(List<String> messages) {
        List<Future<RecordMetadata>> sentRecors = new LinkedList<>();
        LOGGER.info("initialize kafka producer; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        producer = createProducer(vertx, bootstrapHost, clientID, clientSecret);

        messages.forEach(message -> {
            LOGGER.info("send messgae {} to topic: {}", message, topic);
            sentRecors.add(producer.send(KafkaProducerRecord.create(topic, message)));
        });
        return sentRecors;
    }

    static public KafkaProducer<String, String> createProducer(
            Vertx vertx, String bootstrapHost, String clientID, String clientSecret) {

        Map<String, String> config = KafkaUtils.configs(bootstrapHost, clientID, clientSecret);
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("acks", "all");

        return KafkaProducer.create(vertx, config);
    }

    public Future<Void> close() {
        if (vertx != null && producer != null) {
            return producer.close()
                    .onSuccess(v -> LOGGER.info("Producer closed"))
                    .onFailure(cause -> fail("Producer not closed", cause));
        }
        return Future.succeededFuture(null);
    }
}
