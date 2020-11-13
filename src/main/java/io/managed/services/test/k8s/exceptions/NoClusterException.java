package io.managed.services.test.k8s.exceptions;

public class NoClusterException extends Exception {
    public NoClusterException(String message) {
        super(message);
    }
}
