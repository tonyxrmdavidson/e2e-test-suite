package io.managed.services.test.client.exception;

import java.util.List;
import java.util.Map;

public class ApiUnknownException extends Exception {

    private final int code;
    private final Map<String, List<String>> responseHeaders;
    private final String responseBody;

    public ApiUnknownException(
        String message,
        int code, Map<String, List<String>> responseHeaders,
        String responseBody,
        Exception cause) {

        super(message, cause);
        this.code = code;
        this.responseHeaders = responseHeaders;
        this.responseBody = responseBody;
    }

    public int getCode() {
        return code;
    }

    public Map<String, List<String>> getResponseHeaders() {
        return responseHeaders;
    }

    public String getResponseBody() {
        return responseBody;
    }

    public String getFullMessage() {
        var error = new StringBuilder();
        error.append(getMessage());
        error.append(String.format("\nStatus Code: %d", getCode()));
        for (var e : getResponseHeaders().entrySet()) {
            error.append(String.format("\n< %s: %s", e.getKey(), e.getValue()));
        }
        error.append(String.format("\n%s", getResponseBody()));
        return error.toString();
    }
}
