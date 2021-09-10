package io.managed.services.test.client.exception;

public class ApiForbiddenException extends ApiGenericException {
    public ApiForbiddenException(ApiUnknownException e) {
        super(e);
    }
}
