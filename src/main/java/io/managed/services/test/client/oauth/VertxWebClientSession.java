package io.managed.services.test.client.oauth;

import io.managed.services.test.client.exception.ResponseException;
import io.vertx.core.Future;
import io.vertx.ext.web.client.WebClientSession;

public class VertxWebClientSession {

    private final WebClientSession session;

    public VertxWebClientSession(WebClientSession session) {
        this.session = session;
    }

    public VertxHttpRequest getAbs(String absoluteURI) {
        return new VertxHttpRequest(session.getAbs(absoluteURI), absoluteURI);
    }

    public VertxHttpRequest postAbs(String absoluteURI) {
        return new VertxHttpRequest(session.postAbs(absoluteURI), absoluteURI);
    }

    public static Future<VertxHttpResponse> assertResponse(VertxHttpResponse response, Integer statusCode) {
        if (response.statusCode() != statusCode) {
            String message = String.format("Expected status code %d but got %s", statusCode, response.statusCode());
            return Future.failedFuture(new ResponseException(message, response));
        }
        return Future.succeededFuture(response);
    }
}
