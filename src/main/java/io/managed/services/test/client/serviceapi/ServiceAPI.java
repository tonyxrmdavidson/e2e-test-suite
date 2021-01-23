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
import java.util.function.Function;


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

    Function<HttpResponse<Buffer>, Future<HttpResponse<Buffer>>> assertResponse(Integer statusCode) {
        return (HttpResponse<Buffer> response) -> {
            if (response.statusCode() != statusCode) {

                StringBuilder error = new StringBuilder();
                error.append(String.format("Expected status code %d but got %s", statusCode, response.statusCode()));
                for (Map.Entry<String, String> e : response.headers().entries()) {
                    error.append(String.format("\n< %s: %s", e.getKey(), e.getValue()));
                }
                error.append(String.format("\n%s", response.bodyAsString()));
                return Future.failedFuture(error.toString());
            }
            return Future.succeededFuture(response);
        };
    }

    public Future<KafkaRequest> createKafka(KafkaRequestPayload payload) {
        return client.post("/api/managed-services-api/v1/kafkas")
                .authentication(token)
                .sendJson(payload)
                .flatMap(assertResponse(200))
                .map(response -> response.bodyAsJson(KafkaRequest.class));
    }

    public Future<Void> deleteKafka(String id) {
        return client.delete(String.format("/api/managed-services-api/v1/kafkas/%s", id))
                .authentication(token)
                .send()
                .flatMap(assertResponse(200))
                .map(response -> response.bodyAsJson(Void.class));
    }
}


