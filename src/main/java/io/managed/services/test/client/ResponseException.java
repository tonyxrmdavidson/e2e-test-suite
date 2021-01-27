package io.managed.services.test.client;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;

import java.util.Map;

public class ResponseException extends Exception {

    public static ResponseException create(String message, HttpResponse<Buffer> response) {
        StringBuilder error = new StringBuilder();
        error.append(message);
        for (Map.Entry<String, String> e : response.headers().entries()) {
            error.append(String.format("\n< %s: %s", e.getKey(), e.getValue()));
        }
        error.append(String.format("\n%s", response.bodyAsString()));
        return new ResponseException(error.toString());
    }

    ResponseException(String message) {
        super(message);
    }
}
