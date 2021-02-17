package io.managed.services.test.cli;

public class ProcessException extends Exception {
    public final Process process;

    ProcessException(String message, Process process) {
        super(message);
        this.process = process;
    }
}
