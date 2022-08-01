package io.managed.services.test.client.kafkamgmt;

import com.openshift.cloud.api.kas.models.InstantQuery;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import io.managed.services.test.ThrowingFunction;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.observatorium.ObservatoriumClient;
import io.managed.services.test.observatorium.ObservatoriumException;
import io.managed.services.test.observatorium.QueryResult;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.waitFor;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopic;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;

public class KafkaMgmtMetricsUtils {
    private static final Logger LOGGER = LogManager.getLogger(KafkaMgmtMetricsUtils.class);

    private static final String IN_MESSAGES_METRIC = "kafka_server_brokertopicmetrics_messages_in_total";
    private static final int MESSAGE_COUNT = 17;
    private static final Duration WAIT_FOR_METRIC_TIMEOUT = Duration.ofMinutes(5);

    /**
     * Return the sum for all values of a single metric and topic
     *
     * @param metricItems List<InstantQuery>
     * @param topicName   String
     * @param metric      String
     * @return double
     */
    public static double collectTopicMetric(List<InstantQuery> metricItems, String topicName, String metric) {
        Objects.requireNonNull(metricItems);
        return metricItems.stream()
            .filter(item -> item.getMetric() != null)
            .filter(item -> metric.equals(item.getMetric().get("__name__")))
            .filter(item -> topicName.equals(item.getMetric().get("topic")))
            .mapToDouble(i -> i.getValue())
            .sum();
    }

    public static void testMessageInTotalMetric(
        KafkaMgmtApi api,
        KafkaRequest kafka,
        ServiceAccount serviceAccount,
        String topicName)
        throws Throwable {

        LOGGER.info("start testing message in total metric");

        // retrieve the current in messages before sending more
        var metricsList = api.getMetricsByInstantQuery(kafka.getId(), null);
        var initialInMessages = collectTopicMetric(metricsList.getItems(), topicName, IN_MESSAGES_METRIC);
        LOGGER.info("the topic '{}' started with '{}' in messages", topicName, initialInMessages);

        // send n messages to the topic
        LOGGER.info("send '{}' message to the topic '{}'", MESSAGE_COUNT, topicName);
        bwait(testTopic(
            Vertx.vertx(),
            kafka.getBootstrapServerHost(),
            serviceAccount.getClientId(),
            serviceAccount.getClientSecret(),
            topicName,
            MESSAGE_COUNT,
            10,
            100));

        // wait for the metric to be updated or fail with timeout
        var finalInMessagesAtom = new AtomicReference<Double>();
        ThrowingFunction<Boolean, Boolean, ApiGenericException> isMetricUpdated = last -> {

            var m = api.getMetricsByInstantQuery(kafka.getId(), null);
            var i = collectTopicMetric(m.getItems(), topicName, IN_MESSAGES_METRIC);

            finalInMessagesAtom.set(i);

            LOGGER.debug("kafka_server_brokertopicmetrics_messages_in_total: {}", i);
            return initialInMessages + MESSAGE_COUNT == i;
        };
        waitFor("metric to be updated", ofSeconds(3), WAIT_FOR_METRIC_TIMEOUT, isMetricUpdated);

        LOGGER.info("final in message count for topic '{}' is: {}", topicName, finalInMessagesAtom.get());
    }


    @SneakyThrows
    public static <T extends Throwable> void waitUntilExpectedMetricRange(ObservatoriumClient observatoriumClient, String kafkaId, String metricName, double previouslyObservedValue, double expectedIncrease, double errorRangePercentage) {

        ThrowingFunction<Boolean, Boolean, T> ready = last -> {
            ObservatoriumClient.Query query = new ObservatoriumClient.Query();
            query.metric(metricName).label("_id", kafkaId);
            QueryResult result = null;
            try {
                result = observatoriumClient.query(query);
            } catch (ObservatoriumException e) {
                e.printStackTrace();
            }
            Double newStorageTotal = result.data.result.get(0).doubleValue();


            // Compare if observedIncrease (i.e., increase in metric from beginning of observation) falls within range of expected increase.

            double observedIncrease = newStorageTotal - previouslyObservedValue;
            LOGGER.info("currently observed increase: {}", observedIncrease);
            // how many %from expected data, are observed far from. e.g. expect 200 bytes, observed 80, difference is - 60 (%).
            var differencePercentage = (observedIncrease - expectedIncrease) / (expectedIncrease / 100);
            LOGGER.info("currently observed difference %: {}", differencePercentage);
            // information about if there was too little/ many data produced is important as well, so check from both side is made
            var isReady = differencePercentage > -errorRangePercentage && differencePercentage < errorRangePercentage;

            LOGGER.info("is metric data within expected range: {}", isReady);
            return isReady;
        };

        try {
            waitFor("metric to be ready", ofSeconds(6), ofMinutes(2), ready);
        } catch (TimeoutException e) {
            // throw a more accurate error
            throw new ObservatoriumException("metric not ready within expected time");
        }

        LOGGER.info("Metric is ready");
    }

}



