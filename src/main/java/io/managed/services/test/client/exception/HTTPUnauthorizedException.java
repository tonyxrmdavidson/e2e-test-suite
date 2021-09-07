package io.managed.services.test.client.exception;

import io.vertx.ext.web.client.HttpResponse;

@Deprecated
public class HTTPUnauthorizedException extends ResponseException {

    public HTTPUnauthorizedException(String message, HttpResponse<?> response) {
        super(message, response);
    }
}
