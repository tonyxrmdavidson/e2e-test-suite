package io.managed.services.test.client.kafkamgmt;

import io.managed.services.test.client.exception.ApiToManyRequestsException;

public class KafkaToManyRequestsException extends Exception {
    public KafkaToManyRequestsException(ApiToManyRequestsException e) {
        super(e);
    }
}
