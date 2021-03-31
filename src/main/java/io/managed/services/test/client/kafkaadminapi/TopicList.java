package io.managed.services.test.client.kafkaadminapi;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class TopicList {
    @JsonProperty("items")
    public List<Topic> topics;
    public int offset;
    public int limit;
    public int count;


}
