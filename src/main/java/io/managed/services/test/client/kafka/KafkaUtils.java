package io.managed.services.test.client.kafka;

import io.managed.services.test.Environment;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;


public class KafkaUtils {
    private static final Logger LOGGER = LogManager.getLogger(KafkaUtils.class);

    static public Map<String, String> plainConfigs(String bootstrapHost, String clientID, String clientSecret) {
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", bootstrapHost);
        config.put("sasl.mechanism", "PLAIN");
        config.put("security.protocol", "SASL_SSL");
        config.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", clientID, clientSecret));
        return config;
    }

    static public Map<String, String> configs(String bootstrapHost, String clientID, String clientSecret) {
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", bootstrapHost);
        config.put("sasl.mechanism", "OAUTHBEARER");
        config.put("security.protocol", "SASL_SSL");
        String jaas = String.format("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.client.id=\"%s\" oauth.client.secret=\"%s\" " +
                "oauth.token.endpoint.uri=\"%s/auth/realms/%s/protocol/openid-connect/token\";", clientID, clientSecret, Environment.MAS_SSO_REDHAT_KEYCLOAK_URI, Environment.MAS_SSO_REDHAT_REALM);
        config.put("sasl.jaas.config", jaas);
        config.put("sasl.login.callback.handler.class", "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
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
        return admin.getMapOfTopicNameAndDescriptionByName(name)
                .map(r -> r.get(name))
                .recover(t -> {
                    LOGGER.error("topic not found:", t);
                    return Future.succeededFuture(null);
                })
                .map(Optional::ofNullable);
    }

}
