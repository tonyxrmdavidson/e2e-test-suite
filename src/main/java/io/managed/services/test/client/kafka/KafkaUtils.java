package io.managed.services.test.client.kafka;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static io.managed.services.test.TestUtils.forEach;


public class KafkaUtils {
    private static final Logger LOGGER = LogManager.getLogger(KafkaUtils.class);

    static public Map<String, String> configs(String bootstrapHost, String clientID, String clientSecret) {
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", bootstrapHost);
        config.put("sasl.mechanism", "PLAIN");
        config.put("security.protocol", "SASL_SSL");
        config.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", clientID, clientSecret));
        return config;
    }

    static public <T> CompletionStage<T> toCompletionStage(KafkaFuture<T> future) {
        CompletableFuture<T> completable = new CompletableFuture<>();
        future.whenComplete((r, e) -> {
            if (e == null) {
                completable.complete(r);
            } else {
                completable.completeExceptionally(e);
            }
        });
        return completable;
    }

    static public <T> Future<T> toVertxFuture(KafkaFuture<T> future) {
        Promise<T> promise = Promise.promise();

        future.whenComplete((r, e) -> {
            if (e == null) {
                promise.complete(r);
            } else {
                promise.fail(e);
            }
        });
        return promise.future();
    }

    static public Future<Optional<TopicDescription>> getTopicByName(KafkaAdmin admin, String name) {
        return toVertxFuture(admin.getMapOfTopicNameAndDescriptionByName(name))
                .map(r -> r.get(name))
                .recover(t -> {
                    LOGGER.error("topic not found:", t);
                    return Future.succeededFuture(null);
                })
                .map(Optional::ofNullable);
    }

    /**
     * Create the missing topics
     *
     * @return the list of missing topics that has been created
     */
    static public Future<List<String>> applyTopics(KafkaAdmin admin, Set<String> topics) {

        List<String> missingTopics = new ArrayList<>();

        return toVertxFuture(admin.listTopics())

                // create the missing topics
                .compose(currentTopics -> forEach(topics.iterator(), t -> {
                    if (currentTopics.contains(t)) {
                        currentTopics.remove(t);
                        return Future.succeededFuture();
                    }

                    missingTopics.add(t);

                    LOGGER.info("create missing topic: {}", t);
                    return toVertxFuture(admin.createTopic(t));
                }).map(v -> missingTopics));
    }
}
