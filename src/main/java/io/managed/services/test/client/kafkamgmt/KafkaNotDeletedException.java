package io.managed.services.test.client.kafkamgmt;

import com.openshift.cloud.api.kas.models.KafkaRequest;

public class KafkaNotDeletedException extends Exception {

    public KafkaNotDeletedException(KafkaRequest k, Exception cause) {
        super("kafka instance is not deleted\n" + k.toString(), cause);
    }
}
