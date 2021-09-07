package io.managed.services.test.client.kafkaadminapi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@Deprecated
@JsonIgnoreProperties(ignoreUnknown = true)
public  class Topic {
    public String name;
    public boolean isInternal;
    public List<TopicPartition> partitions;
    public List<TopicConfig> config;
}
