package io.managed.services.test.client.kafkaadminapi;

import io.managed.services.test.client.BaseVertxClient;
import io.managed.services.test.client.kafkaadminapi.resourcesGroups.GroupResponse;
import io.managed.services.test.client.kafkaadminapi.resourcesgroup.CreateTopicPayload;
import io.managed.services.test.client.kafkaadminapi.resourcesgroup.Topic;
import io.managed.services.test.client.kafkaadminapi.resourcesgroup.TopicList;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.web.client.HttpResponse;

import java.net.HttpURLConnection;
import java.util.ArrayList;

public class KafkaAdminAPI extends BaseVertxClient {

    final TokenCredentials token;

    public KafkaAdminAPI(Vertx vertx, String uri, String token) {
        super(vertx, uri);
        this.token = new TokenCredentials(token);
    }

    public KafkaAdminAPI(Vertx vertx, String uri, User user) {
        this(vertx, uri, KeycloakOAuth.getToken(user));
    }


    public Future<Void> createTopic(String topicName) {
        CreateTopicPayload topicPayload = setUpDefaultTopicPayload(topicName);
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

    private CreateTopicPayload setUpDefaultTopicPayload(String name) {
        CreateTopicPayload topicPayload = new CreateTopicPayload();
        topicPayload.name = name;
        topicPayload.settings = new CreateTopicPayload.Settings();

        // Partitions needs to be set to 1 in order sending messages properly in test suite ServiceApiLongLiveTest
        topicPayload.settings.numPartitions = 1;
        CreateTopicPayload.Settings.Config c1 = new CreateTopicPayload.Settings.Config();
        CreateTopicPayload.Settings.Config c2 = new CreateTopicPayload.Settings.Config();
        c1.key = "min.insync.replicas";
        c1.value = "1";
        c2.key = "max.message.bytes";
        c2.value = "1050000";
        topicPayload.settings.config = new ArrayList<>();
        topicPayload.settings.config.add(c1);
        topicPayload.settings.config.add(c2);
        return topicPayload;
    }

}

