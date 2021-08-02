package io.managed.services.test.client.exception;

public class ApiUnauthorizedException extends ApiException {
    public ApiUnauthorizedException(com.openshift.cloud.api.srs.invoker.ApiException apiException) {
        super(apiException);
    }
}
