package io.managed.services.test.client.kafka;

import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

public class ConsumerRecord<K, V> {
    private final int consumerHash;
    private final KafkaConsumerRecord<K, V> record;

    public ConsumerRecord(int consumerHash, KafkaConsumerRecord<K, V> record) {
        this.consumerHash = consumerHash;
        this.record = record;
    }

    public int consumerHash() {
        return consumerHash;
    }

    public KafkaConsumerRecord<K, V> record() {
        return record;
    }
}
