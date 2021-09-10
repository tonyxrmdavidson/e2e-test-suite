package io.managed.services.test.client.kafkamgmt;

import com.openshift.cloud.api.kas.models.KafkaRequest;

public class KafkaNotReadyException extends Exception {

    public KafkaNotReadyException(KafkaRequest k) {
        this(k, null);
    }

    public KafkaNotReadyException(KafkaRequest k, Exception cause) {
        super("kafka instance is not ready\n" + k.toString(), cause);
    }
}
