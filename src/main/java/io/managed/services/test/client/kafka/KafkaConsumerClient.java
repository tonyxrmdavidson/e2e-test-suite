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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class KafkaConsumerClient {
    private static final Logger LOGGER = LogManager.getLogger(KafkaConsumerClient.class);
    private final Vertx vertx;
    private final KafkaConsumer<String, String> consumer;
    private final Object lock = new Object();

    public KafkaConsumerClient(Vertx vertx, String bootstrapHost, String clientID, String clientSecret, boolean oauth, boolean wantLatestPartitionOffset) {
        this.vertx = vertx;

        LOGGER.info("initialize kafka consumer; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        if (oauth) {
            consumer = createConsumer(vertx, bootstrapHost, clientID, clientSecret, wantLatestPartitionOffset);
        } else {
            consumer = createConsumerWithPlain(vertx, bootstrapHost, clientID, clientSecret, wantLatestPartitionOffset);
        }

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

    public static KafkaConsumer<String, String> createConsumerWithPlain(
            Vertx vertx, String bootstrapHost, String clientID, String clientSecret, boolean wantLatestPartitionOffset) {
        String offset = wantLatestPartitionOffset ? "latest" : "earliest";
        Map<String, String> config = KafkaUtils.plainConfigs(bootstrapHost, clientID, clientSecret);
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("group.id", "test-group");
        config.put("auto.offset.reset", offset);
        config.put("enable.auto.commit", "true");

        return KafkaConsumer.create(vertx, config);
    }


    public static KafkaConsumer<String, String> createConsumer(
            Vertx vertx, String bootstrapHost, String clientID, String clientSecret, boolean wantLatestPartitionOffset) {
        return createConsumer(vertx, bootstrapHost, clientID, clientSecret, "test-group", wantLatestPartitionOffset);
    }

    public static KafkaConsumer<String, String> createConsumer(
            Vertx vertx, String bootstrapHost, String clientID, String clientSecret, String groupID, boolean wantLatestPartitionOffset) {
        Map<String, String> config = KafkaUtils.configs(bootstrapHost, clientID, clientSecret);
        //Standard consumer config
        String offset = wantLatestPartitionOffset ? "latest" : "earliest";
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("group.id", groupID);
        config.put("auto.offset.reset", offset);
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

    public  static Future<List<KafkaConsumer<String, String>>> createNConsumers(Vertx vertx, int n, String topic, String bootstrapHost, String clientID, String clientSecret, String groupID) {
        LOGGER.info("creating {} Consumers", n);
        Promise<List<KafkaConsumer<String, String>>> completionPromise = Promise.promise();
        AtomicInteger startedConsumersCounter = new AtomicInteger(0);
        List<KafkaConsumer<String, String>> consumerList = new LinkedList<>();


        for (int i = 0; i < n; i++) {
            KafkaConsumer<String, String> consumer =  KafkaConsumerClient.createConsumer(
                    vertx, bootstrapHost, clientID,  clientSecret,  groupID, false);
            consumerList.add(consumer);
            int finalI = i;
            consumer
                    .subscribe(topic)
                    .onSuccess(__ -> {
                        LOGGER.info("{} consumer subscribed", finalI);
                        if (startedConsumersCounter.incrementAndGet() == n) {
                            LOGGER.info("all consumers are listening");
                            completionPromise.complete(consumerList);
                        }
                    })
                    .onFailure(cause -> {
                        LOGGER.error("some of consumers didn't subscribe: {}", cause.getMessage());
                        completionPromise.fail("consumer unable to start");
                    });
        }
        return completionPromise.future();
    }

    public  static Future<List<PartitionConsumerTuple>> handleListOfConsumers(List<KafkaConsumer<String, String>> consumerList) {
        Promise<List<PartitionConsumerTuple>> completionPromise = Promise.promise();
        List<PartitionConsumerTuple> unsafeList = new LinkedList<>();
        List<PartitionConsumerTuple> partitionConsumerTupleList = Collections.synchronizedList(unsafeList);

        AtomicInteger obtainedMessagesCounter = new AtomicInteger(0);
        int i = 0;
        for (KafkaConsumer<String, String> consumer: consumerList) {
            i++;
            int finalI = i;
            LOGGER.info("consumer {} starts listening", finalI);
            consumer.handler(record -> {
                obtainedMessagesCounter.getAndIncrement();
                LOGGER.info("consumer {} : consume partition {}, with offset: {}", finalI, record.partition(), record.offset());
                PartitionConsumerTuple pt = new PartitionConsumerTuple(record.partition(), finalI);
                partitionConsumerTupleList.add(pt);

                if (obtainedMessagesCounter.get() == 20) {
                    LOGGER.info("20 messages obtained");
                    completionPromise.complete(partitionConsumerTupleList);
                }
            });
        }
        return completionPromise.future();
    }

    public static Future<Void> closeListOfConsumer(List<KafkaConsumer<String, String>> consumerList) {
        Promise<Void> promise = Promise.promise();
        AtomicInteger closedConsumersCount = new AtomicInteger(0);
        int consumersCount = consumerList.size();
        for (KafkaConsumer<String, String> consumer: consumerList) {
            consumer.close()
                    .onSuccess(__ -> {
                        if (closedConsumersCount.incrementAndGet() == consumersCount) {
                            LOGGER.info("all consumers closed");
                            promise.complete();
                        }
                    })
                    .onFailure(e -> promise.fail(e.getMessage()));
        }
        return promise.future();
    }
}


