package io.managed.services.test.client.serviceapi;

import io.managed.services.test.client.BaseVertxClient;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.TokenCredentials;


public class ServiceAPI extends BaseVertxClient {

    final User user;
    final TokenCredentials token;

    public ServiceAPI(Vertx vertx, String uri, User user) {
        super(vertx, uri);
        this.user = user;
        this.token = new TokenCredentials(KeycloakOAuth.getToken(user));
    }


    public Future<KafkaResponse> createKafka(CreateKafkaPayload payload, Boolean async) {
        return client.post("/api/managed-services-api/v1/kafkas")
                .authentication(token)
                .addQueryParam("async", async.toString())
                .sendJson(payload)
                .compose(r -> assertResponse(r, 202))
                .map(r -> r.bodyAsJson(KafkaResponse.class));
    }

    public Future<KafkaListResponse> getListOfKafkaByName(String name) {
        return client.get("/api/managed-services-api/v1/kafkas")
                .authentication(token)
                .addQueryParam("page", "1")
                .addQueryParam("size", "10")
                .addQueryParam("search", String.format("name = %s", name.trim()))
                .send()
                .compose(r -> assertResponse(r, 200))
                .map(r -> r.bodyAsJson(KafkaListResponse.class));
    }

    public Future<KafkaListResponse> getListOfKafkas() {
        return client.get("/api/managed-services-api/v1/kafkas")
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, 200))
                .map(r -> r.bodyAsJson(KafkaListResponse.class));
    }

    public Future<KafkaResponse> getKafka(String id) {
        return client.get(String.format("/api/managed-services-api/v1/kafkas/%s", id))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, 200))
                .map(r -> r.bodyAsJson(KafkaResponse.class));
    }

    public Future<Void> deleteKafka(String id, Boolean async) {
        return client.delete(String.format("/api/managed-services-api/v1/kafkas/%s?async=%s", id, async))
                .authentication(token)
                .addQueryParam("async", async.toString())
                .send()
                .compose(r -> assertResponse(r, async ? 202 : 204))
                .map(r -> r.bodyAsJson(Void.class));
    }

    public Future<ServiceAccount> createServiceAccount(CreateServiceAccountPayload payload) {
        return client.post("/api/managed-services-api/v1/serviceaccounts")
                .authentication(token)
                .sendJson(payload)
                .compose(r -> assertResponse(r, 202)) // TODO: report issue: swagger says 200
                .map(r -> r.bodyAsJson(ServiceAccount.class));
    }

    public Future<ServiceAccountList> getListOfServiceAccounts() {
        return client.get("/api/managed-services-api/v1/serviceaccounts")
            .authentication(token)
            .send()
            .compose(r -> assertResponse(r, 200))
            .map(r -> r.bodyAsJson(ServiceAccountList.class));
    }

    public Future<Void> deleteServiceAccount(String id) {
        return client.delete(String.format("/api/managed-services-api/v1/serviceaccounts/%s", id))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, 204))
                .map(r -> r.bodyAsJson(Void.class));
    }
}


