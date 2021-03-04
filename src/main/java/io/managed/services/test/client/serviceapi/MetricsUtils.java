package io.managed.services.test.client.serviceapi;

import java.util.List;

public class MetricsUtils {

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
                .reduce((double) 0, (__, item) -> item.value, (a, b) -> a + b);
    }
}
