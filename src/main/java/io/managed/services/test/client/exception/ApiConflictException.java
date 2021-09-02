package io.managed.services.test.client.exception;

public class ApiConflictException extends ApiGenericException {
    public ApiConflictException(ApiUnknownException e) {
        super(e);
    }
}
