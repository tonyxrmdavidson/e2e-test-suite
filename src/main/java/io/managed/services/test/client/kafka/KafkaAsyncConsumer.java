package io.managed.services.test.client.kafka;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.List;
import java.util.Map;

@Log4j2
abstract class KafkaAsyncConsumer<K, V> implements AutoCloseable {

    abstract Future<Future<List<ConsumerRecord<K, V>>>> receiveAsync(String topicName, int expectedMessages);

    abstract Future<Void> asyncClose();

    protected static <K, V> KafkaConsumer<K, V> createConsumer(
        Vertx vertx,
        String bootstrapHost,
        String clientID,
        String clientSecret,
        KafkaAuthMethod authMethod,
        String groupID,
        String offset,
        Class<? extends Deserializer<K>> keyDeserializer,
        Class<? extends Deserializer<V>> valueDeserializer,
        Map<String, String> additionalConfig) {

        var config = authMethod.configs(bootstrapHost, clientID, clientSecret);

        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getName());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        // add the additional configs
        config.putAll(additionalConfig);

        return KafkaConsumer.create(vertx, config);
    }

    @Override
    public void close() throws Exception {
        log.warn("force closing kafka consumer(s)");
        asyncClose().toCompletionStage().toCompletableFuture().get();
    }
}
