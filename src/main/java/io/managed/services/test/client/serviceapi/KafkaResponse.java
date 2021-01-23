package io.managed.services.test.client.serviceapi;

import com.fasterxml.jackson.annotation.JsonProperty;

public class KafkaResponse {
    public String id;
    public String kind;
    public String href;
    public String status;
    @JsonProperty("cloud_provider")
    public String cloudProvider;
    @JsonProperty("multi_az")
    public Boolean multiAZ;
    public String region;
    public String owner;
    public String name;
    public String bootstrapServerHost;
    @JsonProperty("created_at")
    public String createdAt;
    @JsonProperty("updated_at")
    public String updatedAt;
    @JsonProperty("failed_reason")
    public String failedReason;
}
