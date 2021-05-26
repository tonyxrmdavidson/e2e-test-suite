package io.managed.services.test.client.kafka;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.List;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.LinkedList;
import static io.managed.services.test.TestUtils.forEach;

public class KafkaConsumerClient implements KafkaAsyncConsumer {
    private static final Logger LOGGER = LogManager.getLogger(KafkaConsumerClient.class);
    private final Vertx vertx;
    private final KafkaConsumer<String, String> consumer;

    public KafkaConsumerClient(
        Vertx vertx,
        String bootstrapHost,
        String clientID,
        String clientSecret,
        KafkaAuthMethod method) {
        this(vertx, bootstrapHost, clientID, clientSecret, method, "test-group", "latest");
    }

    public KafkaConsumerClient(
        Vertx vertx,
        String bootstrapHost,
        String clientID,
        String clientSecret,
        KafkaAuthMethod method,
        String groupID,
        String offset) {

        this.vertx = vertx;

        LOGGER.info("initialize kafka consumer; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        consumer = createConsumer(vertx, bootstrapHost, clientID, clientSecret, method, groupID, offset);
    }

    @Override
    public Future<Future<List<ConsumerRecord>>> receiveAsync(String topicName, int expectedMessages) {

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
    public static Future<Void> resetToEnd(KafkaConsumer<String, String> consumer, String topic) {

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

    public  static Future<KafkaConsumer<String, String>> subscribeConsumer(KafkaConsumer<String, String> consumer, String topic) {
        LOGGER.info("Subscribing consumer");
        Promise<KafkaConsumer<String, String>> completionPromise = Promise.promise();
        consumer
            .subscribe(topic)
            .onSuccess(__ -> completionPromise.complete(consumer))
            .onFailure(cause -> {
                LOGGER.error("Consumers didn't subscribe: {}", cause.getMessage());
                completionPromise.fail("consumer unable to subscribe");
            });
        return completionPromise.future();
    }

    public  static Future<Void> consumeSingleMessage(KafkaConsumer<String, String> consumer) {
        // it is necessary to complete promise only once, otherwise caused Exception, already completed promise.
        AtomicInteger obtainedMessagesCounter = new AtomicInteger(0);
        Promise<Void> completionPromise = Promise.promise();
        consumer.handler(record -> {
            if (obtainedMessagesCounter.incrementAndGet() == 1) completionPromise.complete();
        });
        return completionPromise.future();
    }

    public static Future<Void> closeSingleConsumer(KafkaConsumer<String, String> consumerList) {
        LOGGER.info("closing consumed");
        Promise<Void> promise = Promise.promise();
        consumerList.close()
            .onSuccess(__ -> promise.complete())
            .onFailure(e -> promise.fail(e.getMessage()));
        return promise.future();
    }


    private Future<List<ConsumerRecord>> consumeMessages(int expectedMessages) {
        Promise<List<ConsumerRecord>> promise = Promise.promise();
        List<ConsumerRecord> messages = new LinkedList<>();

        // set the fetch batch to the expected messages
        consumer.fetch(expectedMessages);

        consumer.handler(record -> {
            messages.add(new ConsumerRecord(consumer.hashCode(), record));
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

        var config = authMethod.configs(bootstrapHost, clientID, clientSecret);

        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("group.id", groupID);
        config.put("auto.offset.reset", offset);
        config.put("enable.auto.commit", "true");

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
