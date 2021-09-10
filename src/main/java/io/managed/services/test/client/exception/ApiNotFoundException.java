package io.managed.services.test.client.exception;

public class ApiNotFoundException extends ApiGenericException {
    public ApiNotFoundException(ApiUnknownException e) {
        super(e);
    }
}
