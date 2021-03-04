package io.managed.services.test.client.serviceapi;

import com.fasterxml.jackson.annotation.JsonProperty;

public class KafkaUserMetric {
    @JsonProperty("__name__")
    public String name;

    @JsonProperty("strimzi_io_cluster")
    public String strimziCluster;

    public String topic;

    @JsonProperty("statefulset_kubernetes_io_pod_name")
    public String podName;

    @JsonProperty("exported_pod")
    public String exportedPod;

    @JsonProperty("exported_service")
    public String exportedService;

    public String route;

    @JsonProperty("persistentvolumeclaim")
    public String persistentVolumeClaim;
}
