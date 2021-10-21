package io.managed.services.test.client.kafkamgmt;

import io.managed.services.test.client.exception.ApiForbiddenException;

public class KafkaToManyRequestsException extends Exception {
    public KafkaToManyRequestsException(ApiForbiddenException e) {
        super(e);
    }
}
