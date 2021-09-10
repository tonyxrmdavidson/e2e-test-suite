package io.managed.services.test.client.kafkamgmt;

import java.util.Arrays;
import java.util.List;

public class KafkaUnknownHostsException extends Exception {

    public KafkaUnknownHostsException(List<String> hosts, Exception cause) {
        super("failed to resolve kafka hosts '{}'" + Arrays.deepToString(hosts.toArray()), cause);
    }
}
