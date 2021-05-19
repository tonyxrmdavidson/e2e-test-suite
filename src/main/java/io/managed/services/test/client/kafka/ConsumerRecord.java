package io.managed.services.test.client.kafka;

import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

public class ConsumerRecord {
    private final int consumerHash;
    private final KafkaConsumerRecord<String, String> record;

    public ConsumerRecord(int consumerHash, KafkaConsumerRecord<String, String> record) {
        this.consumerHash = consumerHash;
        this.record = record;
    }

    public int consumerHash() {
        return consumerHash;
    }

    public KafkaConsumerRecord<String, String> record() {
        return record;
    }
}

