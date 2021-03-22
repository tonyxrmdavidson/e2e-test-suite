package io.managed.services.test.client.kafka;

import io.vertx.core.Future;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.managed.services.test.client.kafka.KafkaUtils.toVertxFuture;

public class KafkaAdmin {

    public final Admin admin;

    public KafkaAdmin(String bootstrapHost, String clientID, String clientSecret) {

        Map<String, Object> conf = KafkaUtils.configs(bootstrapHost, clientID, clientSecret)
                .entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        admin = Admin.create(conf);
    }

    public Future<Void> createTopic(String name) {
        return createTopic(name, null, null);
    }

    public Future<Void> createTopic(String name, Integer partitions, Short replicas) {
        NewTopic topic = new NewTopic(name, Optional.ofNullable(partitions), Optional.ofNullable(replicas));
        return toVertxFuture(admin.createTopics(Collections.singleton(topic)).all());
    }

    public Future<Set<String>> listTopics() {
        return toVertxFuture(admin.listTopics().names());
    }

    public Future<Map<String, TopicDescription>> getMapOfTopicNameAndDescriptionByName(String name) {
        return toVertxFuture(admin.describeTopics(Collections.singleton(name)).all());
    }

    public Future<Void> deleteTopic(String name) {
        return toVertxFuture(admin.deleteTopics(Collections.singleton(name)).all());
    }

    public void close() {
        admin.close(Duration.ofSeconds(3));
    }
}


