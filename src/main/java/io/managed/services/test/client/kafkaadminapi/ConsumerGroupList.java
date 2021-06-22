package io.managed.services.test.client.kafkaadminapi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsumerGroupList {
    public List<ConsumerGroup> items;
    public int offset;
    public int limit;
    public int count;
}
