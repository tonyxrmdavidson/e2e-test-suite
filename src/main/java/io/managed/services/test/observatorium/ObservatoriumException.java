package io.managed.services.test.observatorium;

public class ObservatoriumException extends Exception {
    public ObservatoriumException(Exception wrapped) {
        super(wrapped);
    }

    public ObservatoriumException(String reason) {
        super(reason);
    }
}
