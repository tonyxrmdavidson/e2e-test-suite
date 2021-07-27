package io.managed.services.test.client.exception;

import java.net.HttpURLConnection;
import java.util.List;
import java.util.Map;

public class ApiException extends Exception {

    private final com.openshift.cloud.api.srs.invoker.ApiException apiException;

    public ApiException(com.openshift.cloud.api.srs.invoker.ApiException apiException) {
        super(message(apiException), apiException);
        this.apiException = apiException;
    }

    public static ApiException apiException(com.openshift.cloud.api.srs.invoker.ApiException apiException) {
        switch (apiException.getCode()) {
            case HttpURLConnection.HTTP_NOT_FOUND:
                return new ApiNotFoundException(apiException);
            case HttpURLConnection.HTTP_CONFLICT:
            case HttpURLConnection.HTTP_FORBIDDEN:
            case HttpURLConnection.HTTP_UNAUTHORIZED:
            case 423:
            default:
                return new ApiException(apiException);
        }
    }

    public com.openshift.cloud.api.srs.invoker.ApiException getApiException() {
        return apiException;
    }

    static private <T> String message(com.openshift.cloud.api.srs.invoker.ApiException apiException) {
        StringBuilder error = new StringBuilder();
        error.append(apiException.getMessage());
        error.append(String.format("\nStatus Code: %d", apiException.getCode()));
        for (Map.Entry<String, List<String>> e : apiException.getResponseHeaders().entrySet()) {
            error.append(String.format("\n< %s: %s", e.getKey(), e.getValue()));
        }
        error.append(String.format("\n%s", apiException.getResponseBody()));
        return error.toString();
    }
}
