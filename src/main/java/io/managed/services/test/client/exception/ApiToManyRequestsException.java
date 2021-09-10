package io.managed.services.test.client.exception;

public class ApiToManyRequestsException extends ApiGenericException {
    public ApiToManyRequestsException(ApiUnknownException e) {
        super(e);
    }
}
