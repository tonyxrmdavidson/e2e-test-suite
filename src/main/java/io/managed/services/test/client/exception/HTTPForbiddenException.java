package io.managed.services.test.client.exception;

import io.vertx.ext.web.client.HttpResponse;

@Deprecated
public class HTTPForbiddenException extends ResponseException {

    public HTTPForbiddenException(String message, HttpResponse<?> response) {
        super(message, response);
    }
}
