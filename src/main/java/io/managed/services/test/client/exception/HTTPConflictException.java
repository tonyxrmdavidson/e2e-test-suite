package io.managed.services.test.client.exception;

import io.vertx.ext.web.client.HttpResponse;

@Deprecated
public class HTTPConflictException extends ResponseException {

    public HTTPConflictException(String message, HttpResponse<?> response) {
        super(message, response);
    }
}
