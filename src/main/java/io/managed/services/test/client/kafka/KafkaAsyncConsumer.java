package io.managed.services.test.client.kafka;

import io.vertx.core.Future;

import java.util.List;

public interface KafkaAsyncConsumer {

    Future<Future<List<ConsumerRecord>>> receiveAsync(String topicName, int expectedMessages);

    Future<Void> close();
}
