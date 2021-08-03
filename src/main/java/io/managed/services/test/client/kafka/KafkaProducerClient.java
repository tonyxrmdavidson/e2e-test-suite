package io.managed.services.test.client.kafka;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaProducerClient<K, V> {
    private static final Logger LOGGER = LogManager.getLogger(KafkaProducerClient.class);
    private final Vertx vertx;
    private final KafkaProducer<K, V> producer;

    public KafkaProducerClient(
        Vertx vertx,
        String bootstrapHost,
        String clientID,
        String clientSecret,
        KafkaAuthMethod authMethod,
        Class<? extends Deserializer<K>> keyDeserializer,
        Class<? extends Deserializer<V>> valueDeserializer) {

        this(vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            authMethod,
            keyDeserializer,
            valueDeserializer,
            new HashMap<>());
    }

    public KafkaProducerClient(
        Vertx vertx,
        String bootstrapHost,
        String clientID,
        String clientSecret,
        KafkaAuthMethod authMethod,
        Class<? extends Deserializer<K>> keyDeserializer,
        Class<? extends Deserializer<V>> valueDeserializer,
        Map<String, String> additionalConfig) {

        this.vertx = vertx;

        LOGGER.info("initialize kafka producer; host: {}; clientID: {}; clientSecret: ***", bootstrapHost, clientID);
        producer = createProducer(
            vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            authMethod,
            keyDeserializer,
            valueDeserializer,
            additionalConfig);
    }

    public Future<List<RecordMetadata>> sendAsync(String topicName, List<V> messages) {

        List<Future> sent = messages.stream()
            .map(message -> producer.send(KafkaProducerRecord.create(topicName, message)))
            .collect(Collectors.toList());

        return CompositeFuture.all(sent)
            .onComplete(__ -> LOGGER.info("successfully sent {} messages to topic: {}", messages.size(), topicName))
            .map(c -> c.list());
    }

    public static <K, V> KafkaProducer<K, V> createProducer(
        Vertx vertx,
        String bootstrapHost,
        String clientID,
        String clientSecret,
        KafkaAuthMethod method,
        Class<? extends Deserializer<K>> keyDeserializer,
        Class<? extends Deserializer<V>> valueDeserializer,
        Map<String, String> additionalConfig) {

        var config = method.configs(bootstrapHost, clientID, clientSecret);

        // Standard consumer config
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keyDeserializer.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueDeserializer.getName());
        config.put(ProducerConfig.ACKS_CONFIG, "all");

        // add the additional configs
        config.putAll(additionalConfig);

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
