package io.managed.services.test.client.exception;

public class ApiLockedException extends ApiGenericException {
    public ApiLockedException(ApiUnknownException e) {
        super(e);
    }
}
