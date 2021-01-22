package io.managed.services.test;


import com.fasterxml.jackson.annotation.JsonProperty;

public class KafkaRequestPayload {
    public String name;
    @JsonProperty("cloud_provider")
    public String cloudProvider;
    public String region;
}