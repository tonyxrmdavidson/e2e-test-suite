package io.managed.services.test.client.kafka;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaProducerClient<K, V> implements AutoCloseable {
    private static final Logger LOGGER = LogManager.getLogger(KafkaProducerClient.class);
    private final KafkaProducer<K, V> producer;

    public KafkaProducerClient(
        Vertx vertx,
        String bootstrapHost,
        String clientID,
        String clientSecret,
        KafkaAuthMethod authMethod,
        Class<? extends Serializer<K>> keySerializer,
        Class<? extends Serializer<V>> valueSerializer) {

        this(vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            authMethod,
            keySerializer,
            valueSerializer,
            new HashMap<>());
    }

    public KafkaProducerClient(
        Vertx vertx,
        String bootstrapHost,
        String clientID,
        String clientSecret,
        KafkaAuthMethod authMethod,
        Class<? extends Serializer<K>> keySerializer,
        Class<? extends Serializer<V>> valueSerializer,
        Map<String, String> additionalConfig) {

        LOGGER.info("initialize kafka producer; host: {}; clientID: {}; clientSecret: ***", bootstrapHost, clientID);
        producer = createProducer(
            vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            authMethod,
            keySerializer,
            valueSerializer,
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

    private static <K, V> KafkaProducer<K, V> createProducer(
        Vertx vertx,
        String bootstrapHost,
        String clientID,
        String clientSecret,
        KafkaAuthMethod method,
        Class<? extends Serializer<K>> keySerializer,
        Class<? extends Serializer<V>> valueSerializer,
        Map<String, String> additionalConfig) {

        var config = method.configs(bootstrapHost, clientID, clientSecret);

        // Standard consumer config
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getName());
        config.put(ProducerConfig.ACKS_CONFIG, "all");

        // add the additional configs
        config.putAll(additionalConfig);

        return KafkaProducer.create(vertx, config);
    }

    public Future<RecordMetadata> send(KafkaProducerRecord<K, V> record) {
        return producer.send(record);
    }

    public Future<Void> asyncClose() {
        return producer.close()
            .onSuccess(v -> LOGGER.info("KafkaProducerClient closed"))
            .onFailure(c -> LOGGER.error("failed to close KafkaProducerClient", c));
    }

    @Override
    public void close() throws Exception {
        LOGGER.warn("force closing KafkaConsumerClient");
        asyncClose().toCompletionStage().toCompletableFuture().get();
    }
}
