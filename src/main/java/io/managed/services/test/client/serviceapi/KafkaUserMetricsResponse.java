package io.managed.services.test.client.serviceapi;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;



public class KafkaUserMetricsResponse {
    public String id;
    public String kind;
    public List<KafkaUserMetricItem> items;
}
