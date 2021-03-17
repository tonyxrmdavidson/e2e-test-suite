package io.managed.services.test.client.kafkaadminapi.resourcesgroup;

import java.util.List;

public  class Topic {
    public String name;
    public boolean isInternal;
    public List<Partition> partitions;
    public List<Config> config;

    public static class Partition {
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

    public static class Config{
        public String key;
        public String value;
    }
}
