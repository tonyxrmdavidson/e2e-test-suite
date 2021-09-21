package io.managed.services.test.client.kafka;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.managed.services.test.TestUtils.message;
import static org.slf4j.helpers.MessageFormatter.format;


public class KafkaMessagingUtils {
    private static final Logger LOGGER = LogManager.getLogger(KafkaMessagingUtils.class);

    public static Future<Void> testTopic(
        Vertx vertx,
        String bootstrapHost,
        String clientID,
        String clientSecret,
        String topicName,
        int messageCount,
        int minMessageSize,
        int maxMessageSize) {

        return testTopic(vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            topicName,
            messageCount,
            minMessageSize,
            maxMessageSize,
            KafkaAuthMethod.OAUTH);
    }

    public static Future<Void> testTopic(
        Vertx vertx,
        String bootstrapHost,
        String clientID,
        String clientSecret,
        String topicName,
        int messageCount,
        int minMessageSize,
        int maxMessageSize,
        KafkaAuthMethod authMethod) {

        return testTopic(vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            topicName,
            Duration.ofMinutes(1),
            messageCount,
            minMessageSize,
            maxMessageSize,
            authMethod);
    }

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
        Duration timeout,
        int messageCount,
        int minMessageSize,
        int maxMessageSize,
        KafkaAuthMethod authMethod) {

        // generate random strings to send as messages
        var messages = generateRandomMessages(messageCount, minMessageSize, maxMessageSize);

        // initialize the consumer and the producer
        var consumer = new KafkaConsumerClient<>(vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            authMethod,
            StringDeserializer.class,
            StringDeserializer.class);

        var producer = new KafkaProducerClient<>(
            vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            authMethod,
            StringSerializer.class,
            StringSerializer.class);

        return produceAndConsumeMessages(vertx, producer, consumer, topicName, timeout, messages)
            .compose(records -> assertRecords(messages, records));
    }


    /**
     * Create a producer and multiple consumers for the kafka instance and send n random messages from
     * the producer to the consumers and validate that each message reach the destination
     *
     * @param vertx             Vertx
     * @param bootstrapHost     Kafka bootstrapHost
     * @param clientID          Service Account ID
     * @param clientSecret      Service Account Secret
     * @param topicName         Topic Name
     * @param messageCount      Number of Messages to send
     * @param minMessageSize    The min number of characters to use when generating the random messages
     * @param maxMessageSize    The max number of characters to use when generating the random messages
     * @param numberOfConsumers The max number of consumers to create
     * @return Future
     */
    public static Future<Void> testTopicWithMultipleConsumers(
        Vertx vertx,
        String bootstrapHost,
        String clientID,
        String clientSecret,
        String topicName,
        Duration timeout,
        int messageCount,
        int minMessageSize,
        int maxMessageSize,
        int numberOfConsumers) {

        var authMethod = KafkaAuthMethod.OAUTH;
        var groupID = "multi-consumer-test";

        // generate random strings to send as messages
        var messages = generateRandomMessages(messageCount, minMessageSize, maxMessageSize);

        // initialize the consumer and the producer
        var consumer = new KafkaConsumerClientPool<>(
            vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            groupID,
            authMethod,
            numberOfConsumers,
            StringDeserializer.class,
            StringDeserializer.class);

        var producer = new KafkaProducerClient<>(
            vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            authMethod,
            StringSerializer.class,
            StringSerializer.class);

        return produceAndConsumeMessages(vertx, producer, consumer, topicName, timeout, messages)

            // assert the records
            .compose(records -> assertRecords(messages, records)
                .compose(__ -> assertUnusedConsumers(consumer, records)));
    }

    private static Future<List<ConsumerRecord<String, String>>> produceAndConsumeMessages(
        Vertx vertx,
        KafkaProducerClient<String, String> producer,
        KafkaAsyncConsumer<String, String> consumer,
        String topicName,
        Duration timeout,
        List<String> messages) {

        LOGGER.info("start listening for {} messages on topic {}", messages.size(), topicName);

        return consumer.receiveAsync(topicName, messages.size())
            .compose(consumeFuture -> {
                LOGGER.info("start sending {} messages on topic {}", messages.size(), topicName);
                var produceFuture = producer.sendAsync(topicName, messages);

                var timeoutPromise = Promise.promise();
                var timeoutTimer = vertx.setTimer(timeout.toMillis(), __ -> {
                    LOGGER.error("timeout after {} waiting for {} messages on topic {}", timeout, messages.size(), topicName);
                    timeoutPromise.fail(message("timeout after {} waiting for {} messages on topic: {}", timeout, messages.size(), topicName));
                });

                var completeFuture = CompositeFuture.join(produceFuture, consumeFuture)
                    .onComplete(__ -> {
                        vertx.cancelTimer(timeoutTimer);
                        timeoutPromise.tryComplete();
                    });

                var completeOrTimeoutFuture = CompositeFuture.all(completeFuture, timeoutPromise.future());

                return completeOrTimeoutFuture.map(__ -> {
                    LOGGER.info("producer and consumer has complete for topic {}", topicName);

                    var records = consumeFuture.result();
                    LOGGER.info("received {} messages on topic {}", records.size(), topicName);

                    return records;
                });

            })
            .eventually(__ -> {
                // close the producer and consumer in any case
                LOGGER.info("close the consumer and the producer for topic {}", topicName);
                return CompositeFuture.join(producer.asyncClose(), consumer.asyncClose());
            });
    }

    public static List<String> generateRandomMessages(int messageCount, int minMessageSize, int maxMessageSize) {
        return IntStream.range(0, messageCount)
            .boxed()
            .map(v -> RandomStringUtils.random((int) random(minMessageSize, maxMessageSize), true, true))
            .collect(Collectors.toList());
    }

    private static Future<Void> assertUnusedConsumers(
        KafkaConsumerClientPool<String, String> pool,
        List<ConsumerRecord<String, String>> records) {

        var consumerHashes = pool.getConsumers().stream().map(c -> c.hashCode()).collect(Collectors.toList());

        for (var record : records) {
            var i = consumerHashes.indexOf(record.consumerHash());
            if (i != -1) {
                consumerHashes.remove(i);
            }
        }

        if (consumerHashes.isEmpty()) {
            return Future.succeededFuture();
        }

        var message = message("not all consumers has received at least one message; unused-consumers: {}", consumerHashes);
        return Future.failedFuture(new AssertionError(message));
    }

    private static Future<Void> assertRecords(List<String> expectedMessages, List<ConsumerRecord<String, String>> receivedRecords) {
        return assertMessages(expectedMessages, receivedRecords.stream().map(r -> r.record().value()).collect(Collectors.toList()));
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
        return Future.failedFuture(new AssertionError(message));
    }

    public static long random(long from, long to) {
        return (long) (Math.random() * ((to - from) + 1)) + from;
    }

}
