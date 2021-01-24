package io.managed.services.test.client.serviceapi;

import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;

import java.net.URI;
import java.util.Map;


public class ServiceAPI {

    final Vertx vertx;
    final WebClient client;
    final User user;
    final TokenCredentials token;

    public ServiceAPI(Vertx vertx, String uri, User user) {

        URI u = URI.create(uri);
        this.vertx = vertx;
        this.client = WebClient.create(vertx, new WebClientOptions()
                .setDefaultHost(u.getHost())
                .setDefaultPort(getPort(u))
                .setSsl(isSsl(u)));
        this.user = user;
        this.token = new TokenCredentials(KeycloakOAuth.getToken(user));
    }

    static Integer getPort(URI u) {
        if (u.getPort() == -1) {
            switch (u.getScheme()) {
                case "https":
                    return 443;
                case "http":
                    return 80;
            }
        }
        return u.getPort();
    }

    static Boolean isSsl(URI u) {
        switch (u.getScheme()) {
            case "https":
                return true;
            case "http":
                return false;
        }
        return false;
    }

    Future<HttpResponse<Buffer>> assertResponse(HttpResponse<Buffer> response, Integer statusCode) {
        if (response.statusCode() != statusCode) {

            StringBuilder error = new StringBuilder();
            error.append(String.format("Expected status code %d but got %s", statusCode, response.statusCode()));
            for (Map.Entry<String, String> e : response.headers().entries()) {
                error.append(String.format("\n< %s: %s", e.getKey(), e.getValue()));
            }
            error.append(String.format("\n%s", response.bodyAsString()));
            return Future.failedFuture(new Exception(error.toString()));
        }
        return Future.succeededFuture(response);
    }

    public Future<KafkaResponse> createKafka(CreateKafkaPayload payload, Boolean async) {
        return client.post("/api/managed-services-api/v1/kafkas")
                .authentication(token)
                .addQueryParam("async", async.toString())
                .sendJson(payload)
                .compose(r -> assertResponse(r, 202))
                .map(r -> r.bodyAsJson(KafkaResponse.class));
    }

    public Future<KafkaResponse> getKafka(String id) {
        return client.get(String.format("/api/managed-services-api/v1/kafkas/%s", id))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, 200))
                .map(r -> r.bodyAsJson(KafkaResponse.class));
    }

    public Future<Void> deleteKafka(String id) {
        return client.delete(String.format("/api/managed-services-api/v1/kafkas/%s", id))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, 204))
                .map(r -> r.bodyAsJson(Void.class));
    }

    public Future<ServiceAccount> createServiceAccount(CreateServiceAccountPayload payload) {
        return client.post("/api/managed-services-api/v1/serviceaccounts")
                .authentication(token)
                .sendJson(payload)
                .compose(r -> assertResponse(r, 202)) // TODO: report issue: swagger says 200
                .map(r -> r.bodyAsJson(ServiceAccount.class));
    }

    public Future<Void> deleteServiceAccount(String id) {
        return client.delete(String.format("/api/managed-services-api/v1/serviceaccounts/%s", id))
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, 204))
                .map(r -> r.bodyAsJson(Void.class));
    }
}


