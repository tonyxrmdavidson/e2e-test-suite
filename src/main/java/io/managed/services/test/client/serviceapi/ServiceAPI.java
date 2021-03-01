package io.managed.services.test.client.serviceapi;

import io.managed.services.test.client.BaseVertxClient;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.TokenCredentials;

import java.net.HttpURLConnection;


public class ServiceAPI extends BaseVertxClient {

    final TokenCredentials token;

    public ServiceAPI(Vertx vertx, String uri, String token) {
        super(vertx, uri);
        this.token = new TokenCredentials(token);
    }

    public ServiceAPI(Vertx vertx, String uri, User user) {
        this(vertx, uri, KeycloakOAuth.getToken(user));
    }


    public Future<KafkaResponse> createKafka(CreateKafkaPayload payload, Boolean async) {
        return retry(() -> client.post("/api/managed-services-api/v1/kafkas")
                .authentication(token)
                .addQueryParam("async", async.toString())
                .sendJson(payload)
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_ACCEPTED))
                .map(r -> r.bodyAsJson(KafkaResponse.class)));
    }

    public Future<KafkaListResponse> getListOfKafkaByName(String name) {
        return retry(() -> client.get("/api/managed-services-api/v1/kafkas")
                .authentication(token)
                .addQueryParam("page", "1")
                .addQueryParam("size", "10")
                .addQueryParam("search", String.format("name = %s", name.trim()))
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(KafkaListResponse.class)));
    }

    public Future<KafkaListResponse> getListOfKafkas() {
        return retry(() -> client.get("/api/managed-services-api/v1/kafkas")
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(KafkaListResponse.class)));
    }

    public Future<KafkaResponse> getKafka(String id) {
        return retry(() -> client.get(String.format("/api/managed-services-api/v1/kafkas/%s", id))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(KafkaResponse.class)));
    }

    public Future<Void> deleteKafka(String id, Boolean async) {
        return retry(() -> client.delete(String.format("/api/managed-services-api/v1/kafkas/%s?async=%s", id, async))
                .authentication(token)
                .addQueryParam("async", async.toString())
                .send()
                .compose(r -> assertResponse(r, async ? HttpURLConnection.HTTP_ACCEPTED : HttpURLConnection.HTTP_NO_CONTENT))
                .map(r -> r.bodyAsJson(Void.class)));
    }

    public Future<ServiceAccount> createServiceAccount(CreateServiceAccountPayload payload) {
        return retry(() -> client.post("/api/managed-services-api/v1/serviceaccounts")
                .authentication(token)
                .sendJson(payload)
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_ACCEPTED)) // TODO: report issue: swagger says 200
                .map(r -> r.bodyAsJson(ServiceAccount.class)));
    }

    public Future<ServiceAccountList> getListOfServiceAccounts() {
        return retry(() -> client.get("/api/managed-services-api/v1/serviceaccounts")
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(ServiceAccountList.class)));
    }

    public Future<ServiceAccount> resetCredentialsServiceAccount(String id) {
        return retry(() -> client.post(String.format("/api/managed-services-api/v1/serviceaccounts/%s/reset-credentials", id))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(ServiceAccount.class)));
    }

    public Future<Void> deleteServiceAccount(String id) {
        return retry(() -> client.delete(String.format("/api/managed-services-api/v1/serviceaccounts/%s", id))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_NO_CONTENT))
                .map(r -> r.bodyAsJson(Void.class)));
    }
}


