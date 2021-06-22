package io.managed.services.test.client.kafkaadminapi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TopicPartition {
    public int partition;
    public List<Replica> replicas;
    public List<Isr> isr;
    public Leader leader;

    public static class Leader {
        public int id;
    }

    public static class Replica {
        public int id;
    }

    public static class Isr {
        public int id;
    }
}
