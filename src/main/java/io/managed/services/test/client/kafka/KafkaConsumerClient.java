package io.managed.services.test.client.kafka;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.fail;

public class KafkaConsumerClient {
    private static final Logger LOGGER = LogManager.getLogger(KafkaConsumerClient.class);
    private Vertx vertx;
    private String topic;
    private String bootstrapHost;
    private String clientID;
    private String clientSecret;
    private KafkaConsumer<String, String> consumer;

    public KafkaConsumerClient(Vertx vertx, String topicName, String bootstrapHost, String clientID, String clientSecret) {
        this.vertx = vertx;
        this.topic = topicName;
        this.bootstrapHost = bootstrapHost;
        this.clientID = clientID;
        this.clientSecret = clientSecret;
    }

    public Future<List<KafkaConsumerRecord<String, String>>> receiveAsync(int msgExpected) {
        AtomicInteger msgCount = new AtomicInteger(0);
        Promise<List<KafkaConsumerRecord<String, String>>> received = Promise.promise();
        List<KafkaConsumerRecord<String, String>> msgs = new LinkedList<>();

        LOGGER.info("initialize kafka consumer; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        consumer = createConsumer(vertx, bootstrapHost, clientID, clientSecret);

        consumer.handler(record -> {
            LOGGER.debug("Processing key=" + record.key() + ",value=" + record.value() +
                    ",partition=" + record.partition() + ",offset=" + record.offset());
            msgs.add(record);

            if (msgCount.incrementAndGet() == msgExpected) {
                LOGGER.info("Consumer consumed {} messages", msgCount.get());
                received.complete(msgs);
            }
        });
        LOGGER.info("subscribe to topic: {}", topic);
        return consumer.subscribe(topic)
                .compose(v -> received.future());
    }

    public static KafkaConsumer<String, String> createConsumer(
            Vertx vertx, String bootstrapHost, String clientID, String clientSecret) {

        Map<String, String> config = KafkaUtils.configs(bootstrapHost, clientID, clientSecret);
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("group.id", "test-group");
        config.put("auto.offset.reset", "earliest");
        config.put("enable.auto.commit", "false");

        return KafkaConsumer.create(vertx, config);
    }

    public Future<Void> close() {
        if (vertx != null && consumer != null) {
            return consumer.close()
                    .onSuccess(v -> LOGGER.info("Producer closed"))
                    .onFailure(cause -> fail("Producer not closed", cause));
        }
        return Future.succeededFuture(null);
    }
}
