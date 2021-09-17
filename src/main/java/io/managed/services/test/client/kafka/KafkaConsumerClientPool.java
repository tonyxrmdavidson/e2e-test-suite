package io.managed.services.test.client.kafka;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.managed.services.test.TestUtils.forEach;
import static io.managed.services.test.client.kafka.KafkaConsumerClient.resetToEnd;

public class KafkaConsumerClientPool<K, V> extends KafkaAsyncConsumer<K, V> {
    private static final Logger LOGGER = LogManager.getLogger(KafkaConsumerClientPool.class);

    private final List<KafkaConsumer<K, V>> consumers;

    public KafkaConsumerClientPool(
        Vertx vertx,
        String bootstrapHost,
        String clientID,
        String clientSecret,
        String groupID,
        KafkaAuthMethod authMethod,
        int numberOfConsumer,
        Class<? extends Deserializer<K>> keyDeserializer,
        Class<? extends Deserializer<V>> valueDeserializer) {

        if (numberOfConsumer < 1) {
            throw new InvalidParameterException("the numberOfConsumer can not be smaller then 1");
        }

        consumers = IntStream.range(0, numberOfConsumer)
            .boxed()
            .map(__ -> KafkaConsumerClient.createConsumer(
                vertx,
                bootstrapHost,
                clientID,
                clientSecret,
                authMethod,
                groupID,
                "latest",
                keyDeserializer,
                valueDeserializer,
                new HashMap<>()))
            .collect(Collectors.toList());
    }

    public List<KafkaConsumer<K, V>> getConsumers() {
        return consumers;
    }

    private Future<Void> subscribeAll(String topicName) {

        return forEach(consumers.iterator(), consumer -> {
            LOGGER.info("subscribe consumer {} to topic {}", consumer.hashCode(), topicName);
            return consumer.subscribe(topicName);
        });
    }

    private Future<Void> unsubscribeAll() {
        return forEach(consumers.iterator(), consumer -> {
            LOGGER.info("unsubscribe consumer {}", consumer.hashCode());
            return consumer.unsubscribe();
        });
    }

    private Future<Void> closeAll() {
        return forEach(consumers.iterator(), consumer -> {
            LOGGER.info("close consumer: {}", consumer.hashCode());
            return consumer.close();
        });
    }

    private Future<List<ConsumerRecord<K, V>>> consumeMessages(int expectedMessages) {
        Promise<Void> promise = Promise.promise();
        List<ConsumerRecord<K, V>> records = new LinkedList<>();

        for (var consumer : consumers) {
            var consumerHash = consumer.hashCode();
            LOGGER.info("handle consumer: {}", consumerHash);
            consumer.handler(record -> {
                records.add(new ConsumerRecord<>(consumerHash, record));
                if (records.size() == expectedMessages) {
                    LOGGER.info("successfully received {} messages", expectedMessages);
                    promise.complete();
                }
            });
        }

        return promise.future().map(records);
    }

    public Future<Future<List<ConsumerRecord<K, V>>>> receiveAsync(String topicName, int expectedMessages) {

        // because multiple consumers are still going to connect to a single topic we can just
        // use one consumer to reset all topic partitions
        return resetToEnd(consumers.get(0), topicName)

            .compose(__ -> subscribeAll(topicName))

            .map(__ -> {
                LOGGER.info("consumers successfully subscribed to topic: {}", topicName);

                return consumeMessages(expectedMessages)
                    .compose(r -> unsubscribeAll().map(r));
            });
    }

    @Override
    public Future<Void> asyncClose() {
        return closeAll();
    }
}
