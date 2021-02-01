package io.managed.services.test.client.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class KafkaAdmin {

    final Admin admin;

    public KafkaAdmin(String bootstrapHost, String clientID, String clientSecret) {

        Map<String, Object> conf = KafkaUtils.configs(bootstrapHost, clientID, clientSecret)
                .entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        admin = Admin.create(conf);
    }

    public KafkaFuture<Void> createTopic(String name) {
        return createTopic(name, null, null);
    }

    public KafkaFuture<Void> createTopic(String name, Integer partitions, Short replicas) {
        NewTopic topic = new NewTopic(name, Optional.ofNullable(partitions), Optional.ofNullable(replicas));
        return admin.createTopics(Collections.singleton(topic)).all();
    }

    public KafkaFuture<Map<String, TopicDescription>> getMapOfTopicNameAndDescriptionByName(String name) {
        return admin.describeTopics(Collections.singleton(name)).all();
    }
}


