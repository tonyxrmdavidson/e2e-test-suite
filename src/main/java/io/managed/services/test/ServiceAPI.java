package io.managed.services.test;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;


public class ServiceAPI {

    final Vertx vertx;
    final WebClient client;
    final User user;
    final TokenCredentials token;

    public ServiceAPI(Vertx vertx, User user) {
        this.vertx = vertx;
        this.client = WebClient.create(vertx, new WebClientOptions()
                .setDefaultHost("api.stage.openshift.com")
                .setDefaultPort(443)
                .setSsl(true));
        this.user = user;
        this.token = new TokenCredentials(MASOAuth.getToken(user));
    }

    public Future<KafkaRequest> createKafka(KafkaRequestPayload payload) {
        return client.post("/api/managed-services-api/v1/kafkas")
                .authentication(token)
                .sendJson(payload)
                .map(response -> response.bodyAsJson(KafkaRequest.class));
    }

    public Future<Void> deleteKafka(String id) {
        return client.delete(String.format("/api/managed-services-api/v1/kafkas/%s", id))
                .authentication(token)
                .send()
                .map(response -> response.bodyAsJson(Void.class));
    }
}


