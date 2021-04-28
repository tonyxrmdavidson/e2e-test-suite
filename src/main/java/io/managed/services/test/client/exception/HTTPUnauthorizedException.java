package io.managed.services.test.client.exception;

import io.vertx.ext.web.client.HttpResponse;

public class HTTPUnauthorizedException extends ResponseException {

    public HTTPUnauthorizedException(String message, HttpResponse<?> response) {
        super(message, response);
    }
}
