package io.managed.services.test.client.kafka;

import io.managed.services.test.IsReady;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static io.managed.services.test.TestUtils.message;
import static io.managed.services.test.TestUtils.waitFor;
import static java.time.Duration.ofSeconds;

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
        Promise<List<KafkaConsumerRecord<String, String>>> promise = Promise.promise();
        List<KafkaConsumerRecord<String, String>> messages = new LinkedList<>();

        var close = new Close();
        consumer.handler(record -> {
            synchronized (lock) {
                if (close.isClosed()) {
                    LOGGER.warn("received record while/after unsubscribe from topic");
                    return;
                }
                messages.add(record);
                if (messages.size() == expectedMessages) {
                    close.close();
                    LOGGER.info("successfully received {} messages from topic: {}", expectedMessages, topicName);
                    consumer.unsubscribe()
                            .map(__ -> messages)
                            .onComplete(promise);
                }
            }
        });

        LOGGER.info("subscribe to topic: {}", topicName);
        return consumer.subscribe(topicName)
                .compose(__ -> waitForConsumerToSubscribe(topicName))
                .compose(p -> resetTopicPartitionToEndOffset(p))
                .map(__ -> {
                    LOGGER.info("consumer successfully subscribed to topic: {}", topicName);
                    return promise.future();
                });
    }

    public Future<TopicPartition> waitForConsumerToSubscribe(String topicName) {

        IsReady<TopicPartition> isReady = last -> consumer.assignment().map(partitions -> {
            var o = partitions.stream().filter(p -> p.getTopic().equals(topicName)).findFirst();
            return Pair.with(o.isPresent(), o.orElse(null));
        });

        return waitFor(vertx, message("consumer to subscribe to topic: {}", topicName), ofSeconds(1), ofSeconds(20), isReady);
    }

    public Future<Void> resetTopicPartitionToEndOffset(TopicPartition partition) {
        return consumer.seekToEnd(partition);
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

    final private class Close {
        private boolean close = false;

        synchronized private boolean isClosed() {
            return this.close;
        }

        synchronized private void close() {
            close = true;
        }
    }
}
