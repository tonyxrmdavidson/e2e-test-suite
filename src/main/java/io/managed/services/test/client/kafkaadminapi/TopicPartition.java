package io.managed.services.test.client.kafkaadminapi;

import java.util.List;

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
