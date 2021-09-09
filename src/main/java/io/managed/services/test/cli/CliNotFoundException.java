package io.managed.services.test.cli;

public class CliNotFoundException extends CliGenericException {
    public CliNotFoundException(ProcessException e) {
        super(e);
    }
}
