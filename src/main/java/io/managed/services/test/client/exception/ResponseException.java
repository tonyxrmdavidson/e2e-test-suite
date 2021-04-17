package io.managed.services.test.client.exception;

import io.vertx.ext.web.client.HttpResponse;

import java.net.HttpURLConnection;
import java.util.Map;

public class ResponseException extends Exception {
    public final HttpResponse<?> response;

    protected ResponseException(String message, HttpResponse<?> response) {
        super(message(message, response));
        this.response = response;
    }

    public static ResponseException httpException(String message, HttpResponse<?> response) {
        switch (response.statusCode()) {
            case HttpURLConnection.HTTP_NOT_FOUND:
                return new HTTPNotFoundException(message, response);
            case HttpURLConnection.HTTP_CONFLICT:
                return new HTTPConflictException(message, response);
            case 423:
                return new HTTPLockedException(message, response);
            default:
                return new ResponseException(message, response);
        }
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
