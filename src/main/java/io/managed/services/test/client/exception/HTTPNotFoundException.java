package io.managed.services.test.client.exception;

import io.vertx.ext.web.client.HttpResponse;

@Deprecated
public class HTTPNotFoundException extends ResponseException {

    public HTTPNotFoundException(String message, HttpResponse<?> response) {
        super(message, response);
    }
}
