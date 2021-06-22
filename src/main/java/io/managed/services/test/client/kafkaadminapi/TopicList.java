package io.managed.services.test.client.kafkaadminapi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TopicList {
    public List<Topic> items;
    public int offset;
    public int limit;
    public int count;
    public int size;
    public int total;
    public int page;
}
