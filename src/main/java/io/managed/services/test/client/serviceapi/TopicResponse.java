package io.managed.services.test.client.serviceapi;

import java.util.List;

@Deprecated
public class TopicResponse {
    public List<TopicConfig> config;
    public String name;
    public List<TopicPartition> partitions;
}

@Deprecated
class TopicPartition {
    public int id;
    public List<IDObject> isr;
    public IDObject leader;
    public List<IDObject> replicas;
}

@Deprecated
class IDObject {
    public int id;
}
