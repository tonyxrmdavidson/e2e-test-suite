package io.managed.services.test.client.exception;

public class ApiForbiddenException extends ApiException {
    public ApiForbiddenException(com.openshift.cloud.api.srs.invoker.ApiException apiException) {
        super(apiException);
    }
}
