package io.managed.services.test.client.kafkamgmt;

import io.managed.services.test.client.exception.ApiForbiddenException;

public class KafkaClusterCapacityExhaustedException extends Exception {
    public KafkaClusterCapacityExhaustedException(ApiForbiddenException e) {
        super(e);
    }
}
