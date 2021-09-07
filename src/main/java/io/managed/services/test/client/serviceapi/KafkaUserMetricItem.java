package io.managed.services.test.client.serviceapi;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Deprecated
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaUserMetricItem {

    public KafkaUserMetric metric;

    @JsonProperty("timestamp")
    public Double timestamp;
    @JsonProperty("value")
    public Double value;
}
