package io.managed.services.test.cli;

import java.net.HttpURLConnection;

public class CliNotFoundException extends CliGenericException {
    public CliNotFoundException(ProcessException e) {
        super(e, HttpURLConnection.HTTP_NOT_FOUND);
    }
}
