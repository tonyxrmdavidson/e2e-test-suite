package io.managed.services.test.client.serviceapi;

import io.managed.services.test.IsReady;
import io.managed.services.test.client.kafkaadminapi.KafkaAdminAPIUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;
import org.opentest4j.TestAbortedException;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static io.managed.services.test.TestUtils.message;
import static io.managed.services.test.TestUtils.waitFor;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopicWithOauth;
import static io.managed.services.test.client.kafkaadminapi.KafkaAdminAPIUtils.applyTopics;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.getKafkaByName;
import static java.time.Duration.ofSeconds;

public class MetricsUtils {
    private static final Logger LOGGER = LogManager.getLogger(MetricsUtils.class);
    private static final String TOPIC_NAME = "metric-test-topic";
    private static final String IN_MESSAGES_METRIC = "kafka_server_brokertopicmetrics_messages_in_total";
    private static final int MESSAGE_COUNT = 17;
    private static final Duration WAIT_FOR_METRIC_TIMEOUT = Duration.ofMinutes(2);
    /**
     * Return the sum for all values of a single metric and topic
     *
     * @param metricItems List<KafkaUserMetricItem>
     * @param topicName   String
     * @param metric      String
     * @return double
     */
    public static double collectTopicMetric(List<KafkaUserMetricItem> metricItems, String topicName, String metric) {
        return metricItems.stream()
                .filter(item -> item.metric.name != null)
                .filter(item -> item.metric.topic != null)
                .filter(item -> item.metric.name.equals(metric))
                .filter(item -> item.metric.topic.equals(topicName))
                .reduce((double) 0, (__, item) -> item.value, Double::sum);
    }

    public static Future<Void> messageInTotalMetric(Vertx vertx, ServiceAPI api, String  kafkaInstanceName, ServiceAccount serviceAccount) {
        LOGGER.info("start testing message in total metric");
        Promise<Void> promise = Promise.promise();
        // retrieve the kafka info
        // TODO: Pass the kafka object
        var kafkaF = getKafkaByName(api, kafkaInstanceName)
                .map(o -> o.orElseThrow(() -> new TestAbortedException(message("can't find the long living kafka instance: {}", kafkaInstanceName))));

        var adminF = kafkaF
                .compose(__ -> {
                    var bootstrapHost = kafkaF.result().bootstrapServerHost;
                    return KafkaAdminAPIUtils.kafkaAdminAPI(vertx, bootstrapHost);
                });

        // ensure the topic exists
        var topicF = adminF
                .compose(__ -> {
                    LOGGER.info("ensure the topic {} exists", TOPIC_NAME);
                    return applyTopics(adminF.result(), Set.of(TOPIC_NAME));
                });

        // retrieve the current in messages before sending more
        var initialInMessagesF = topicF
                .compose(__ -> api.queryMetrics(kafkaF.result().id))
                .map(r -> collectTopicMetric(r.items, TOPIC_NAME, IN_MESSAGES_METRIC))
                .onSuccess(i -> LOGGER.info("the topic '{}' started with '{}' in messages", TOPIC_NAME, i));

        // send n messages to the topic
        var testTopicF = initialInMessagesF
                .compose(__ -> {
                    String bootstrapHost = kafkaF.result().bootstrapServerHost;

                    LOGGER.info("send {} message to the topic: {}", MESSAGE_COUNT, TOPIC_NAME);
                    return testTopicWithOauth(vertx, bootstrapHost, serviceAccount.clientID, serviceAccount.clientSecret, TOPIC_NAME, MESSAGE_COUNT, 10, 100);
                });

        // wait for the metric to be updated or fail with timeout
        IsReady<Double> isMetricUpdated = last -> api.queryMetrics(kafkaF.result().id)
                .map(r -> {
                    var in = collectTopicMetric(r.items, TOPIC_NAME, IN_MESSAGES_METRIC);
                    if (last) {
                        LOGGER.warn("last kafka_server_brokertopicmetrics_messages_in_total value: {}", in);
                    }

                    var initialInMessage = initialInMessagesF.result();
                    return Pair.with(initialInMessage + MESSAGE_COUNT == in, in);
                });
        var inMessagesF = testTopicF
                .compose(__ -> waitFor(vertx, "metric to be updated", ofSeconds(3), WAIT_FOR_METRIC_TIMEOUT, isMetricUpdated));

        inMessagesF
                .onSuccess(i -> LOGGER.info("final in message count for topic '{}' is: {}", TOPIC_NAME, i))
                .onSuccess(i -> {
                    if (i != initialInMessagesF.result() + MESSAGE_COUNT) promise.fail(new Exception("count of messages in 2 sources doesn't match"));
                    else {
                        promise.complete();
                    }
                })
                .onFailure(i -> promise.fail("intern error"));

        return promise.future();
    }
}



