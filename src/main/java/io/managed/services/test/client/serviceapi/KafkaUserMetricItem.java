package io.managed.services.test.client.serviceapi;

import com.fasterxml.jackson.annotation.JsonProperty;


public class KafkaUserMetricItem {

    public KafkaUserMetric metric;

    @JsonProperty("Timestamp")
    public Double timestamp;
    @JsonProperty("Value")
    public Double value;
}
