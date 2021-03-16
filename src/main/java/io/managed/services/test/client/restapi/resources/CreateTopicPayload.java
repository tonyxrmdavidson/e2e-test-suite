package io.managed.services.test.client.restapi.resources;

import java.util.List;

public class CreateTopicPayload {
    public String name;
    public Settings settings;


    public static class Settings {
        public int numPartitions;
        public int replicationFactor;
        public List<Config> config;

        public static class Config{
            public String key;
            public String value;
        }
    }
}
