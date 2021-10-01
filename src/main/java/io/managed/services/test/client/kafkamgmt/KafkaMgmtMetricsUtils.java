package io.managed.services.test.client.kafkamgmt;

import com.openshift.cloud.api.kas.models.InstantQuery;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import io.managed.services.test.ThrowingFunction;
import io.managed.services.test.client.exception.ApiGenericException;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.waitFor;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopic;
import static java.time.Duration.ofSeconds;

public class KafkaMgmtMetricsUtils {
    private static final Logger LOGGER = LogManager.getLogger(KafkaMgmtMetricsUtils.class);

    private static final String IN_MESSAGES_METRIC = "kafka_server_brokertopicmetrics_messages_in_total";
    private static final int MESSAGE_COUNT = 17;
    private static final Duration WAIT_FOR_METRIC_TIMEOUT = Duration.ofMinutes(5);

    /**
     * Return the sum for all values of a single metric and topic
     *
     * @param metricItems List<KafkaUserMetricItem>
     * @param topicName   String
     * @param metric      String
     * @return double
     */
    public static double collectTopicMetric(List<InstantQuery> metricItems, String topicName, String metric) {
        Objects.requireNonNull(metricItems);
        return metricItems.stream()
            .filter(item -> item.getMetric() != null)
            .filter(item -> item.getMetric().get("__name__") != null)
            .filter(item -> item.getMetric().get("topic") != null)
            .filter(item -> item.getMetric().get("__name__").equals(metric))
            .filter(item -> item.getMetric().get("topic").equals(topicName))
            .reduce((double) 0, (__, item) -> item.getValue(), Double::sum);
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
}



