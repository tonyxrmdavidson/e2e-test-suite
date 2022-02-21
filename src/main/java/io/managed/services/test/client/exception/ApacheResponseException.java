package io.managed.services.test.client.exception;

import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.io.entity.EntityUtils;

public class ApacheResponseException extends Exception {
    public final CloseableHttpResponse response;

    public ApacheResponseException(Exception cause, CloseableHttpResponse response) {
        super(message(cause.getMessage(), response, null), cause);
        this.response = response;
    }

    public ApacheResponseException(Exception cause, CloseableHttpResponse response, String body) {
        super(message(cause.getMessage(), response, body), cause);
        this.response = response;
    }

    public ApacheResponseException(String message, CloseableHttpResponse response) {
        super(message(message, response, null));
        this.response = response;
    }

    public ApacheResponseException(String message, CloseableHttpResponse response, String body) {
        super(message(message, response, body));
        this.response = response;
    }

    static private String message(String message, CloseableHttpResponse response, String body) {
        StringBuilder error = new StringBuilder();
        error.append(message);
        error.append(String.format("\nStatus Code: %d", response.getCode()));
        for (var e : response.getHeaders()) {
            error.append(String.format("\n< %s: %s", e.getName(), e.getValue()));
        }
        if (body != null) {
            error.append(String.format("\n%s", body));
        } else {
            try {
                error.append(String.format("\n%s", EntityUtils.toString(response.getEntity())));
            } catch (Exception e) {
                error.append(String.format("\nFailed to parse the response body: %s", e.getMessage()));
            }
        }
        return error.toString();
    }
}
