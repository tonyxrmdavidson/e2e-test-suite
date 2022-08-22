package io.managed.services.test.prometheuswebclient;

public class PrometheusException extends Exception {
    public PrometheusException(Exception wrapped) {
        super(wrapped);
    }

    public PrometheusException(String reason) {
        super(reason);
    }
}
