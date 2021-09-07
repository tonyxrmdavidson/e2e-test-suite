package io.managed.services.test.client.serviceapi;

import com.openshift.cloud.api.kas.models.KafkaRequest;
import io.managed.services.test.IsReady;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import java.time.Duration;
import java.util.List;

import static io.managed.services.test.TestUtils.waitFor;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopic;
import static java.time.Duration.ofSeconds;

@Deprecated
public class MetricsUtils {
    private static final Logger LOGGER = LogManager.getLogger(MetricsUtils.class);

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

    public static Future<Void> messageInTotalMetric(Vertx vertx, ServiceAPI api, KafkaRequest kafka, ServiceAccount serviceAccount, String topicName) {
        LOGGER.info("start testing message in total metric");
        Promise<Void> promise = Promise.promise();

        // retrieve the current in messages before sending more
        var initialInMessagesF = api.queryMetrics(kafka.getId())
            .map(r -> collectTopicMetric(r.items, topicName, IN_MESSAGES_METRIC))
            .onSuccess(i -> LOGGER.info("the topic '{}' started with '{}' in messages", topicName, i));

        // send n messages to the topic
        var testTopicF = initialInMessagesF
            .compose(__ -> {
                LOGGER.info("send {} message to the topic: {}", MESSAGE_COUNT, topicName);
                return testTopic(
                    vertx,
                    kafka.getBootstrapServerHost(),
                    serviceAccount.clientID,
                    serviceAccount.clientSecret,
                    topicName,
                    MESSAGE_COUNT,
                    10,
                    100);
            });

        // wait for the metric to be updated or fail with timeout
        IsReady<Double> isMetricUpdated = last -> api.queryMetrics(kafka.getId())
            .map(r -> {
                var in = collectTopicMetric(r.items, topicName, IN_MESSAGES_METRIC);
                if (last) {
                    LOGGER.warn("last kafka_server_brokertopicmetrics_messages_in_total value: {}", in);
                }

                var initialInMessage = initialInMessagesF.result();
                return Pair.with(initialInMessage + MESSAGE_COUNT == in, in);
            });
        var inMessagesF = testTopicF
            .compose(__ -> waitFor(vertx, "metric to be updated", ofSeconds(3), WAIT_FOR_METRIC_TIMEOUT, isMetricUpdated));

        inMessagesF
            .onSuccess(i -> LOGGER.info("final in message count for topic '{}' is: {}", topicName, i))
            .onSuccess(i -> {
                if (i != initialInMessagesF.result() + MESSAGE_COUNT)
                    promise.fail(new Exception("count of messages in 2 sources doesn't match"));
                else {
                    promise.complete();
                }
            })
            .onFailure(i -> promise.fail("intern error"));

        return promise.future();
    }
}



