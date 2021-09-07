package io.managed.services.test.client.kafkaadminapi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@Deprecated
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsumerGroup {
    public String groupId;
    public List<Consumer> consumers;
}
