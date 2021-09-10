package io.managed.services.test.client.exception;

import io.vertx.ext.web.client.HttpResponse;

import java.util.Map;

public class ResponseException extends Exception {
    public final HttpResponse<?> response;

    public ResponseException(String message, HttpResponse<?> response) {
        super(message(message, response));
        this.response = response;
    }

    static private <T> String message(String message, HttpResponse<T> response) {
        StringBuilder error = new StringBuilder();
        error.append(message);
        error.append(String.format("\nStatus Code: %d", response.statusCode()));
        for (Map.Entry<String, String> e : response.headers().entries()) {
            error.append(String.format("\n< %s: %s", e.getKey(), e.getValue()));
        }
        error.append(String.format("\n%s", response.bodyAsString()));
        return error.toString();
    }
}
