package io.managed.services.test.client.kafka;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaConsumerClient {
    private static final Logger LOGGER = LogManager.getLogger(KafkaConsumerClient.class);
    private final Vertx vertx;
    private final KafkaConsumer<String, String> consumer;
    private final Object lock = new Object();

    public KafkaConsumerClient(Vertx vertx, String bootstrapHost, String clientID, String clientSecret) {
        this.vertx = vertx;

        LOGGER.info("initialize kafka consumer; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        consumer = createConsumer(vertx, bootstrapHost, clientID, clientSecret);
    }

    public Future<Future<List<KafkaConsumerRecord<String, String>>>> receiveAsync(String topicName, int expectedMessages) {

        // start by resetting the topic to the end
        return resetToEnd(topicName)

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
    private Future<Void> resetToEnd(String topic) {

        LOGGER.info("rest topic {} offset for all partitions to the end", topic);
        return consumer.partitionsFor(topic)

                // seek the end for all partitions in a topic
                .compose(partitions -> CompositeFuture.all(partitions.stream()
                        .map(partition -> {
                            var tp = new TopicPartition(partition.getTopic(), partition.getPartition());
                            return (Future) consumer.assign(tp)
                                    .compose(__ -> consumer.seekToEnd(tp))

                                    // the seekToEnd take place only once consumer.position() is called
                                    .compose(__ -> consumer.position(tp)
                                            .onSuccess(p -> LOGGER.info("reset partition {}-{} to offset {}", tp.getTopic(), tp.getPartition(), p)));
                        })
                        .collect(Collectors.toList())))

                // commit all partitions offset
                .compose(__ -> consumer.commit())

                // unsubscribe from  all partitions
                .compose(__ -> consumer.unsubscribe());
    }

    private Future<List<KafkaConsumerRecord<String, String>>> consumeMessages(int expectedMessages) {
        Promise<List<KafkaConsumerRecord<String, String>>> promise = Promise.promise();
        List<KafkaConsumerRecord<String, String>> messages = new LinkedList<>();

        // set the fetch batch to the expected messages
        consumer.fetch(expectedMessages);

        consumer.handler(record -> {
            messages.add(record);
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
            Vertx vertx, String bootstrapHost, String clientID, String clientSecret) {

        Map<String, String> config = KafkaUtils.configs(bootstrapHost, clientID, clientSecret);
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("group.id", "test-group");
        config.put("auto.offset.reset", "latest");
        config.put("enable.auto.commit", "true");

        return KafkaConsumer.create(vertx, config);
    }

    public Future<Void> close() {
        if (vertx != null && consumer != null) {
            return consumer.close()
                    .onSuccess(v -> LOGGER.info("KafkaConsumerClient closed"))
                    .onFailure(c -> LOGGER.error("failed to close KafkaConsumerClient", c));
        }
        return Future.succeededFuture(null);
    }
}
