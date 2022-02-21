package io.managed.services.test.client.kafkamgmt;

import com.openshift.cloud.api.kas.models.KafkaRequest;

public class KafkaUnprovisionedException extends Exception {

    public KafkaUnprovisionedException(KafkaRequest k, Exception cause) {
        super("reason: " + k.getFailedReason() + "\n" + k, cause);
    }
}
