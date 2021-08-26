package io.managed.services.test.client.kafka;

import io.vertx.core.Future;

import java.util.List;

public interface KafkaAsyncConsumer<K, V> {

    Future<Future<List<ConsumerRecord<K, V>>>> receiveAsync(String topicName, int expectedMessages);

    Future<Void> close();
}
