package io.managed.services.test.client.kafkaadminapi;

import io.managed.services.test.client.BaseVertxClient;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.TokenCredentials;

import java.net.HttpURLConnection;

/**
 * OpenAPI: https://github.com/bf2fc6cc711aee1a0c2a/kafka-admin-api/blob/main/rest/src/main/resources/openapi-specs/rest.yaml
 */
public class KafkaAdminAPI extends BaseVertxClient {

    final TokenCredentials token;

    public KafkaAdminAPI(Vertx vertx, String uri, String token) {
        super(vertx, uri);
        this.token = new TokenCredentials(token);
    }

    public KafkaAdminAPI(Vertx vertx, String uri, User user) {
        this(vertx, uri, KeycloakOAuth.getToken(user));
    }

    public Future<Topic> createTopic(CreateTopicPayload topicPayload) {
        return retry(() -> client.post("/rest/topics")
                .authentication(token)
                .sendJson(topicPayload)
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_CREATED))
                .map(r -> r.bodyAsJson(Topic.class)));
    }

    public Future<TopicList> getAllTopics() {
        return retry(() -> client.get("/rest/topics")
                .authentication(token).send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(TopicList.class)));
    }

    public Future<Topic> getTopic(String topicName) {
        return retry(() -> client.get(String.format("/rest/topics/%s", topicName))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(Topic.class)));
    }

    public Future<Void> deleteTopic(String topicName) {
        return retry(() -> client.delete(String.format("/rest/topics/%s", topicName))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(Void.class)));
    }

    public Future<ConsumerGroupList> getAllConsumerGroups() {
        return retry(() -> client.get("/rest/consumer-groups")
                .authentication(token).send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(ConsumerGroupList.class)));
    }

    public Future<ConsumerGroup> getConsumerGroup(String id) {
        return retry(() -> client.get(String.format("/rest/consumer-groups/%s", id))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(ConsumerGroup.class)));
    }

    public Future<Void> deleteConsumerGroup(String id) {
        return retry(() -> client.delete(String.format("/rest/consumer-groups/%s", id))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_NO_CONTENT))
                .map(r -> r.bodyAsJson(Void.class)));
    }
}

