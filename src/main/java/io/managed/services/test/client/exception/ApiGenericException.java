package io.managed.services.test.client.exception;

import java.net.HttpURLConnection;

public class ApiGenericException extends Exception {

    private final int code;
    private final String responseBody;

    public ApiGenericException(ApiUnknownException e) {
        super(e.getFullMessage(), e);
        this.code = e.getCode();
        this.responseBody = e.getResponseBody();
    }

    public int getCode() {
        return code;
    }

    public String getResponseBody() {
        return responseBody;
    }

    public static ApiGenericException apiException(ApiUnknownException e) {
        switch (e.getCode()) {
            case HttpURLConnection.HTTP_NOT_FOUND:
                return new ApiNotFoundException(e);
            case HttpURLConnection.HTTP_UNAUTHORIZED:
                return new ApiUnauthorizedException(e);
            case HttpURLConnection.HTTP_FORBIDDEN:
                return new ApiForbiddenException(e);
            case 429:
                return new ApiToManyRequestsException(e);
            case HttpURLConnection.HTTP_CONFLICT:
                return new ApiConflictException(e);
            case 423:
                return new ApiLockedException(e);
            default:
                return new ApiGenericException(e);
        }
    }
}
