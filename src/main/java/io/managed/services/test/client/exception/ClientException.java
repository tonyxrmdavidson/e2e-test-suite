package io.managed.services.test.client.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

public class ClientException extends WebApplicationException {

    public ClientException(String message, Response response) {
        super(message(message, response), response);
    }

    static private <T> String message(String message, Response response) {
        StringBuilder error = new StringBuilder();
        error.append(message);
        error.append(String.format("\nStatus Code: %d", response.getStatus()));
        for (var e : response.getStringHeaders().entrySet()) {
            error.append(String.format("\n< %s: %s", e.getKey(), e.getValue()));
        }
        error.append(String.format("\n%s", response.readEntity(String.class)));
        return error.toString();
    }
}
