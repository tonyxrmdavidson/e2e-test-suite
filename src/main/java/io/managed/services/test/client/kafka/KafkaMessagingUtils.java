package io.managed.services.test.client.kafka;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.slf4j.helpers.MessageFormatter.format;


public class KafkaMessagingUtils {
    private static final Logger LOGGER = LogManager.getLogger(KafkaMessagingUtils.class);

    /**
     * Create a producer and consumer for the kafka instance and send n random messages from
     * the consumer to the producer and validate that each message reach the destination
     *
     * @param vertx          Vertx
     * @param bootstrapHost  Kafka bootstrapHost
     * @param clientID       Service Account ID
     * @param clientSecret   Service Account Secret
     * @param topicName      Topic Name
     * @param messageCount   Number of Messages to send
     * @param minMessageSize The min number of characters to use when generating the random messages
     * @param maxMessageSize The max number of characters to use when generating the random messages
     * @return Future
     */
    public static Future<Void> testTopic(
            Vertx vertx,
            String bootstrapHost,
            String clientID,
            String clientSecret,
            String topicName,
            int messageCount,
            int minMessageSize,
            int maxMessageSize) {

        // generate random strings to send as messages
        var messages = IntStream.range(0, messageCount)
                .boxed()
                .map(v -> RandomStringUtils.random((int) random(minMessageSize, maxMessageSize), true, true))
                .collect(Collectors.toList());

        return produceAndConsumeMessages(vertx, bootstrapHost, clientID, clientSecret, topicName, messages)
                .compose(records -> assertMessages(messages, records));
    }

    private static Future<List<String>> produceAndConsumeMessages(
            Vertx vertx,
            String bootstrapHost,
            String clientID,
            String clientSecret,
            String topicName,
            List<String> messages) {

        // initialize the consumer and the producer
        var consumer = new KafkaConsumerClient(vertx, bootstrapHost, clientID, clientSecret);
        var producer = new KafkaProducerClient(vertx, bootstrapHost, clientID, clientSecret);

        LOGGER.info("start listening for {} messages", messages.size());
        return consumer.receiveAsync(topicName, messages.size()).compose(consumeFuture -> {

            LOGGER.info("start sending {} messages", messages.size());
            var produceFuture = producer.sendAsync(topicName, messages);

            return CompositeFuture.all(produceFuture, consumeFuture).compose(__ -> {

                LOGGER.info("close the consumer and the producer");
                return CompositeFuture.all(producer.close(), consumer.close()).map(___ -> {
                    LOGGER.info("producer and consumer has complete");

                    List<KafkaConsumerRecord<String, String>> records = consumeFuture.result();
                    LOGGER.info("received {} messages", records.size());

                    return records.stream().map(r -> r.value()).collect(Collectors.toList());
                });
            });
        });
    }

    private static Future<Void> assertMessages(List<String> expectedMessages, List<String> receivedMessages) {

        var extraReceivedMessages = receivedMessages.stream()
                .filter(r -> {
                    var i = expectedMessages.indexOf(r);
                    if (i != -1) {
                        expectedMessages.remove(i);
                        return false;
                    }
                    return true;
                })
                .collect(Collectors.toList());

        if (extraReceivedMessages.isEmpty() && expectedMessages.isEmpty()) {
            return Future.succeededFuture();
        }

        var message = format("failed to send all messages or/and received some extra messages;"
                + " not-received-messages: {}, extra-received-messages: {}", expectedMessages, extraReceivedMessages).getMessage();
        return Future.failedFuture(new Exception(message));
    }

    public static long random(long from, long to) {
        return (long) (Math.random() * ((to - from) + 1)) + from;
    }
}
