package io.managed.services.test.client.exception;

public class ApiUnauthorizedException extends ApiGenericException {
    public ApiUnauthorizedException(ApiUnknownException e) {
        super(e);
    }
}
