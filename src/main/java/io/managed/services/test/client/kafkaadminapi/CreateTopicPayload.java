package io.managed.services.test.client.kafkaadminapi;

import java.util.List;

public class CreateTopicPayload {
    public String name;
    public Settings settings;


    public static class Settings {
        public int numPartitions;
        public List<TopicConfig> config;
    }
}
