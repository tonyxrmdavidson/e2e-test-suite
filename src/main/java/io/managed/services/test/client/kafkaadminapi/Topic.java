package io.managed.services.test.client.kafkaadminapi;

import java.util.List;

public  class Topic {
    public String name;
    public boolean isInternal;
    public List<TopicPartition> partitions;
    public List<TopicConfig> config;
}
