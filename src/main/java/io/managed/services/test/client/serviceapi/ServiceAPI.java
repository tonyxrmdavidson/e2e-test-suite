package io.managed.services.test.client.serviceapi;

import io.managed.services.test.client.BaseVertxClient;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.TokenCredentials;

import java.net.HttpURLConnection;

@Deprecated
public class ServiceAPI extends BaseVertxClient {

    final TokenCredentials token;
    private static final String API_BASE = "/api/kafkas_mgmt/v1/";

    public ServiceAPI(Vertx vertx, String uri, String token) {
        super(vertx, uri);
        this.token = new TokenCredentials(token);
    }

    public ServiceAPI(Vertx vertx, String uri, User user) {
        this(vertx, uri, KeycloakOAuth.getToken(user));
    }


    public Future<KafkaResponse> createKafka(CreateKafkaPayload payload, Boolean async) {
        return retry(() -> client.post(API_BASE + "kafkas")
                .authentication(token)
                .addQueryParam("async", async.toString())
                .sendJson(payload)
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_ACCEPTED))
                .map(r -> r.bodyAsJson(KafkaResponse.class)));
    }

    public Future<KafkaListResponse> getListOfKafkaByName(String name) {
        return retry(() -> client.get(API_BASE + "kafkas")
                .authentication(token)
                .addQueryParam("page", "1")
                .addQueryParam("size", "10")
                .addQueryParam("search", String.format("name = %s", name.trim()))
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(KafkaListResponse.class)));
    }

    public Future<KafkaListResponse> getListOfKafkas() {
        return retry(() -> client.get(API_BASE + "kafkas")
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(KafkaListResponse.class)));
    }

    public Future<KafkaResponse> getKafka(String id) {
        return retry(() -> client.get(String.format(API_BASE + "kafkas/%s", id))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(KafkaResponse.class)));
    }

    public Future<Void> deleteKafka(String id, Boolean async) {
        return retry(() -> client.delete(String.format(API_BASE + "kafkas/%s?async=%s", id, async))
                .authentication(token)
                .addQueryParam("async", async.toString())
                .send()
                .compose(r -> assertResponse(r, async ? HttpURLConnection.HTTP_ACCEPTED : HttpURLConnection.HTTP_NO_CONTENT))
                .map(r -> r.bodyAsJson(Void.class)));
    }

    public Future<ServiceAccount> createServiceAccount(CreateServiceAccountPayload payload) {
        return retry(() -> client.post(API_BASE + "service_accounts")
                .authentication(token)
                .sendJson(payload)
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_ACCEPTED)) // TODO: report issue: swagger says 200
                .map(r -> r.bodyAsJson(ServiceAccount.class)));
    }

    public Future<ServiceAccountList> getListOfServiceAccounts() {
        return retry(() -> client.get(API_BASE + "service_accounts")
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(ServiceAccountList.class)));
    }

    public Future<ServiceAccount> resetCredentialsServiceAccount(String id) {
        return retry(() -> client.post(String.format(API_BASE + "service_accounts/%s/reset_credentials", id))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(ServiceAccount.class)));
    }

    public Future<Void> deleteServiceAccount(String id) {
        return retry(() -> client.delete(String.format(API_BASE + "service_accounts/%s", id))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_NO_CONTENT))
                .map(r -> r.bodyAsJson(Void.class)));
    }

    public Future<KafkaUserMetricsResponse> queryMetrics(String kafkaId) {
        return retry(() -> client.get(String.format(API_BASE + "kafkas/%s/metrics/query", kafkaId))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, 200))
                .map(r -> r.bodyAsJson(KafkaUserMetricsResponse.class)));
    }
}


