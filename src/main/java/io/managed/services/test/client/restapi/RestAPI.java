package io.managed.services.test.client.restapi;

import io.managed.services.test.client.BaseVertxClient;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.managed.services.test.client.restapi.resources.CreateTopicPayload;
import io.managed.services.test.client.restapi.resources.Topic;
import io.managed.services.test.client.restapi.resources.Topics;
import io.managed.services.test.client.restapi.resourcesGroups.GroupResponse;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.web.client.HttpResponse;

import java.net.HttpURLConnection;

public class RestAPI extends BaseVertxClient {

    final TokenCredentials token;

    public RestAPI(Vertx vertx, String uri, String token) {
        super(vertx, uri);
        this.token = new TokenCredentials(token);
    }

    public RestAPI(Vertx vertx, String uri, User user) {
        this(vertx, uri, KeycloakOAuth.getToken(user));
    }


    public Future<Topic> createTopic(CreateTopicPayload payload ) {
        return retry(() -> client.post("/rest//topics")
                .authentication(token)
                .sendJson(payload)
                .compose(r -> assertResponse(r,HttpURLConnection.HTTP_CREATED))
                .map(r -> r.bodyAsJson(Topic.class)));
    }

    public Future<String> createTopicAlreadyExisted(CreateTopicPayload payload ) {
        return retry(() -> client.post("/rest//topics")
                .authentication(token)
                .sendJson(payload)
                .compose(r -> assertResponse(r,HttpURLConnection.HTTP_CONFLICT))
                .map(HttpResponse::bodyAsString));
    }

    public Future<Topics> getAllTopics() {
        return retry(() -> client.get("/rest//topics")
                .authentication(token).send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(Topics.class)));
    }

    public Future<Topic> getTopicByName(String topicName) {
        return retry(() -> client.get(String.format("/rest//topics/%s", topicName))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(Topic.class)));
    }

    public Future<String> getTopicByNameNotExisting(String topicName) {
        return retry(() -> client.get(String.format("/rest//topics/%s", topicName))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_NOT_FOUND))
                .map(HttpResponse::bodyAsString));
    }

    public Future<String> deleteTopic(String topicName) {
        return retry(() -> client.delete(String.format("/rest//topics/%s", topicName))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(HttpResponse::bodyAsString));
    }

    public Future<String> deleteTopicNotExisting(String topicName) {
        return retry(() -> client.delete(String.format("/rest//topics/%s", topicName))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_NOT_FOUND))
                .map(HttpResponse::bodyAsString));
    }

    public Future<GroupResponse[]> getAllGroups() {
        return retry(() -> client.get("/rest//groups")
                .authentication(token).send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(GroupResponse[].class )));
    }

    public Future<GroupResponse> getGroupByName(String topicName) {
        return retry(() -> client.get(String.format("/rest//groups/%s", topicName))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(GroupResponse.class)));
    }

    public Future<GroupResponse> getGroupByNameNotExisting(String topicName) {
        return retry(() -> client.get(String.format("/rest//groups/%s", topicName))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(GroupResponse.class)));
    }

    public Future<String> deleteGroup(String topicName) {
        return retry(() -> client.delete(String.format("/rest//groups/%s", topicName))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(HttpResponse::bodyAsString));
    }

    public Future<String> deleteGroupNotExist(String topicName) {
        return retry(() -> client.delete(String.format("/rest//groups/%s", topicName))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(HttpResponse::bodyAsString));
    }
}

