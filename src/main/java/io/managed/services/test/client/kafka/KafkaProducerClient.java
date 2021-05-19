package io.managed.services.test.client.kafka;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import io.vertx.kafka.client.producer.impl.KafkaProducerRecordImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class KafkaProducerClient {
    private static final Logger LOGGER = LogManager.getLogger(KafkaProducerClient.class);
    private final Vertx vertx;
    private final KafkaProducer<String, String> producer;

    public KafkaProducerClient(Vertx vertx, String bootstrapHost, String clientID, String clientSecret, KafkaAuthMethod authMethod) {
        this.vertx = vertx;

        LOGGER.info("initialize kafka producer; host: {}; clientID: {}; clientSecret: ***", bootstrapHost, clientID);
        producer = createProducer(vertx, bootstrapHost, clientID, clientSecret, authMethod);
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

    static public KafkaProducer<String, String> createProducer(
        Vertx vertx, String bootstrapHost, String clientID, String clientSecret, KafkaAuthMethod method) {
        var config = method.configs(bootstrapHost, clientID, clientSecret);

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


    public static Future<Void> produce20Messages(Vertx vertx, String bootstrapHost, String clientID, String clientSecret, String topicName) {
        Promise<Void> p = Promise.promise();
        KafkaProducer<String, String> producer = createProducer(vertx, bootstrapHost, clientID, clientSecret, KafkaAuthMethod.OAUTH);
        for (int i = 1; i <= 20; i++) {
            KafkaProducerRecordImpl<String, String> producerRecord = new KafkaProducerRecordImpl<>(topicName, String.valueOf(i));
            int finalI = i;
            producer.send(producerRecord, recordMetadataAsyncResult -> {
                LOGGER.info("Successfully send message {}, to partition {}", finalI, recordMetadataAsyncResult.result().getPartition());
                if (finalI == 20) {
                    LOGGER.info("All the data have been produced and sent");
                    p.complete();
                }
            });
            producer.flush();
        }
        producer.flush();
        return p.future();
    }
}
