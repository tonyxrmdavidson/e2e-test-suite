package io.managed.services.test.client.serviceapi;

import java.util.List;

@Deprecated
public class KafkaUserMetricsResponse {
    public String id;
    public String kind;
    public List<KafkaUserMetricItem> items;
}
