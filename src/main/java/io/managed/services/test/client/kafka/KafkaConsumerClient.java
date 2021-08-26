package io.managed.services.test.client.kafka;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.managed.services.test.TestUtils.forEach;

public class KafkaConsumerClient<K, V> implements KafkaAsyncConsumer<K, V> {
    private static final Logger LOGGER = LogManager.getLogger(KafkaConsumerClient.class);
    private final Vertx vertx;
    private final KafkaConsumer<K, V> consumer;

    public KafkaConsumerClient(
        Vertx vertx,
        String bootstrapHost,
        String clientID,
        String clientSecret,
        KafkaAuthMethod method,
        Class<? extends Deserializer<K>> keyDeserializer,
        Class<? extends Deserializer<V>> valueDeserializer) {

        this(
            vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            method,
            "test-group",
            "latest",
            keyDeserializer,
            valueDeserializer);
    }

    public KafkaConsumerClient(
        Vertx vertx,
        String bootstrapHost,
        String clientID,
        String clientSecret,
        KafkaAuthMethod method,
        String groupID,
        String offset,
        Class<? extends Deserializer<K>> keyDeserializer,
        Class<? extends Deserializer<V>> valueDeserializer) {

        this(
            vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            method,
            groupID,
            offset,
            keyDeserializer,
            valueDeserializer,
            new HashMap<>());
    }

    public KafkaConsumerClient(
        Vertx vertx,
        String bootstrapHost,
        String clientID,
        String clientSecret,
        KafkaAuthMethod method,
        String groupID,
        String offset,
        Class<? extends Deserializer<K>> keyDeserializer,
        Class<? extends Deserializer<V>> valueDeserializer,
        Map<String, String> additionalConfig) {

        this.vertx = vertx;

        LOGGER.info("initialize kafka consumer; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        consumer = createConsumer(vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            method,
            groupID,
            offset,
            keyDeserializer,
            valueDeserializer,
            additionalConfig);
    }

    @Override
    public Future<Future<List<ConsumerRecord<K, V>>>> receiveAsync(String topicName, int expectedMessages) {

        // start by resetting the topic to the end
        return resetToEnd(consumer, topicName)

            .compose(__ -> {
                LOGGER.info("subscribe to topic: {}", topicName);
                return consumer.subscribe(topicName);
            })

            .map(__ -> {
                LOGGER.info("consumer successfully subscribed to topic: {}", topicName);

                // set the handler and consume the expected messages
                return consumeMessages(expectedMessages);
            });
    }

    /**
     * Subscribe at the end of the topic
     */
    public static <K, V> Future<Void> resetToEnd(KafkaConsumer<K, V> consumer, String topic) {

        LOGGER.info("rest topic {} offset for all partitions to the end", topic);
        return consumer.partitionsFor(topic)

            // seek the end for all partitions in a topic
            .map(partitions -> partitions.stream().map(p -> new TopicPartition(p.getTopic(), p.getPartition())).collect(Collectors.toSet()))
            .compose(partitions -> consumer.assign(partitions)

                .compose(__ -> consumer.seekToEnd(partitions))

                .compose(__ -> forEach(partitions.iterator(), tp -> consumer.position(tp)
                    .map(p -> {
                        LOGGER.info("reset partition {}-{} to offset {}", tp.getTopic(), tp.getPartition(), p);
                        return null;
                    }))))

            // commit all partitions offset
            .compose(__ -> consumer.commit())

            // unsubscribe from  all partitions
            .compose(__ -> consumer.unsubscribe());
    }


    private Future<List<ConsumerRecord<K, V>>> consumeMessages(int expectedMessages) {
        Promise<List<ConsumerRecord<K, V>>> promise = Promise.promise();
        List<ConsumerRecord<K, V>> messages = new LinkedList<>();

        // set the fetch batch to the expected messages
        consumer.fetch(expectedMessages);

        consumer.handler(record -> {
            messages.add(new ConsumerRecord<>(consumer.hashCode(), record));
            if (messages.size() == expectedMessages) {
                LOGGER.info("successfully received {} messages", expectedMessages);
                consumer.commit()
                    .compose(__ -> consumer.unsubscribe())
                    .map(__ -> messages).onComplete(promise);
            }
        });

        return promise.future();
    }

    public static KafkaConsumer<String, String> createConsumer(
        Vertx vertx,
        String bootstrapHost,
        String clientID,
        String clientSecret,
        KafkaAuthMethod authMethod,
        String groupID,
        String offset) {

        return createConsumer(
            vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            authMethod,
            groupID,
            offset,
            StringDeserializer.class,
            StringDeserializer.class,
            new HashMap<>());
    }


    public static <K, V> KafkaConsumer<K, V> createConsumer(
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
    public Future<Void> close() {
        if (vertx != null && consumer != null) {
            return consumer.close()
                .onSuccess(v -> LOGGER.info("KafkaConsumerClient closed"))
                .onFailure(c -> LOGGER.error("failed to close KafkaConsumerClient", c));
        }
        return Future.succeededFuture(null);
    }
}
