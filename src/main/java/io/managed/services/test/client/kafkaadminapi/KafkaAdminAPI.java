package io.managed.services.test.client.kafkaadminapi;

import io.managed.services.test.client.BaseVertxClient;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.web.client.HttpResponse;

import java.net.HttpURLConnection;

public class KafkaAdminAPI extends BaseVertxClient {

    final TokenCredentials token;

    public KafkaAdminAPI(Vertx vertx, String uri, String token) {
        super(vertx, uri);
        this.token = new TokenCredentials(token);
    }

    public KafkaAdminAPI(Vertx vertx, String uri, User user) {
        this(vertx, uri, KeycloakOAuth.getToken(user));
    }


    public Future<Void> createTopic(CreateTopicPayload topicPayload) {
        return retry(() -> client.post("/rest/topics")
                .authentication(token)
                .sendJson(topicPayload)
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_CREATED))
                .map(r -> r.bodyAsJson(Void.class)));
    }

    public Future<TopicList> getAllTopics() {
        return retry(() -> client.get("/rest/topics")
                .authentication(token).send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(TopicList.class)));
    }

    public Future<Topic> getSingleTopicByName(String topicName) {
        return retry(() -> client.get(String.format("/rest/topics/%s", topicName))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(Topic.class)));
    }

    public Future<String> deleteTopicByName(String topicName) {
        return retry(() -> client.delete(String.format("/rest/topics/%s", topicName))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(HttpResponse::bodyAsString));
    }

    public Future<GroupResponse[]> getAllGroups() {
        return retry(() -> client.get("/rest/groups")
                .authentication(token).send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(GroupResponse[].class)));
    }

    public Future<GroupResponse> getSingleGroupByName(String topicName) {
        return retry(() -> client.get(String.format("/rest/groups/%s", topicName))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(GroupResponse.class)));
    }

    public Future<String> deleteGroupByName(String topicName) {
        return retry(() -> client.delete(String.format("/rest/groups/%s", topicName))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(HttpResponse::bodyAsString));
    }



}

