package io.managed.services.test.client.exception;

import io.vertx.ext.web.client.HttpResponse;

public class HTTPLockedException extends ResponseException {

    public HTTPLockedException(String message, HttpResponse<?> response) {
        super(message, response);
    }
}
