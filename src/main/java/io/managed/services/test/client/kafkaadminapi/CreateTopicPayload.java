package io.managed.services.test.client.kafkaadminapi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateTopicPayload {
    public String name;
    public Settings settings;


    public static class Settings {
        public int numPartitions;
        public List<TopicConfig> config;
    }
}
