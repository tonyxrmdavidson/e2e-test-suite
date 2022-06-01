package io.managed.services.test.client.kafka;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.managed.services.test.TestUtils.forEach;

public class KafkaConsumerClient<K, V> extends KafkaAsyncConsumer<K, V> {
    private static final Logger LOGGER = LogManager.getLogger(KafkaConsumerClient.class);
    public final KafkaConsumer<K, V> consumer;

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
                return consumeMessages(expectedMessages)

                    // unsubscribe from the topic after consume all expected messages
                    .compose(messages -> consumer.unsubscribe().map(___ -> messages));
            });
    }

    public Future<Void> resetToEnd(String topic) {
        return resetToEnd(this.consumer, topic);
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

    // Method is almost exactly like consumeMessages but in this case it is public, does not care about data, and is used mostly
    // due to fact that it needs to fail on first sign of fail, it would crash most of other tests (e.g., those that use TestTopic)
    public Future<Void> tryConsumingMessages(int expectedMessages) {
        Promise<Void> promise = Promise.promise();
        AtomicInteger counter = new AtomicInteger();

        // set the fetch batch to the expected messages
        consumer.fetch(expectedMessages);

        consumer.exceptionHandler(e -> {
            LOGGER.error("error while consuming {} messages", expectedMessages);
            promise.fail(e);
        });

        consumer.handler(record -> {
            counter.getAndIncrement();
            if (counter.get() == expectedMessages) {
                LOGGER.info("successfully received {} messages", expectedMessages);
                consumer.commit()
                    .onComplete(promise);
            }
        });

        return promise.future();
    }

    public Future<List<ConsumerRecord<K, V>>> consumeMessages(int expectedMessages) {
        Promise<List<ConsumerRecord<K, V>>> promise = Promise.promise();
        List<ConsumerRecord<K, V>> messages = new LinkedList<>();

        // set the fetch batch to the expected messages
        consumer.fetch(expectedMessages);

        consumer.exceptionHandler(e -> {
            // this handler only means that we are still waiting for the correct data.
            LOGGER.debug("error while consuming {} messages", expectedMessages);
            promise.fail(e);
        });

        consumer.handler(record -> {
            messages.add(new ConsumerRecord<>(consumer.hashCode(), record));
            if (messages.size() == expectedMessages) {
                LOGGER.info("successfully received {} messages", expectedMessages);
                consumer.commit()
                    .map(__ -> messages).onComplete(promise);
            }
        });

        return promise.future();
    }

    public Future<Void> subscribe(String topic) {
        return consumer.subscribe(topic);
    }

    public Future<Void> unsubscribe() {
        return consumer.unsubscribe();
    }

    public void handler(Handler<KafkaConsumerRecord<K, V>> handler) {
        consumer.handler(handler);
    }

    public Future<Set<TopicPartition>> assignment() {
        return consumer.assignment();
    }

    @Override
    public Future<Void> asyncClose() {
        return consumer.close()
            .onSuccess(v -> LOGGER.info("KafkaConsumerClient closed"))
            .onFailure(c -> LOGGER.error("failed to close KafkaConsumerClient", c));
    }
}
