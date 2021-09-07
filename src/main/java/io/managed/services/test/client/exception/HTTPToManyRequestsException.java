package io.managed.services.test.client.exception;

import io.vertx.ext.web.client.HttpResponse;

@Deprecated
public class HTTPToManyRequestsException extends ResponseException {

    public HTTPToManyRequestsException(String message, HttpResponse<?> response) {
        super(message, response);
    }
}
