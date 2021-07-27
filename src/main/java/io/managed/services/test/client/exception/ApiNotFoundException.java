package io.managed.services.test.client.exception;

public class ApiNotFoundException extends ApiException {
    public ApiNotFoundException(com.openshift.cloud.api.srs.invoker.ApiException apiException) {
        super(apiException);
    }
}
