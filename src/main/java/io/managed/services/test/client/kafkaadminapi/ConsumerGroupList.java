package io.managed.services.test.client.kafkaadminapi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@Deprecated
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsumerGroupList {
    public List<ConsumerGroup> items;
    public int offset;
    public int limit;
    public int count;
    public int size;
    public int page;
    public int total;
}
