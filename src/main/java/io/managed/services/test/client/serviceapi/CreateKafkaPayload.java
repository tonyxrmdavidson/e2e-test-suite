package io.managed.services.test.client.serviceapi;


import com.fasterxml.jackson.annotation.JsonProperty;

@Deprecated
public class CreateKafkaPayload {
    public String name;
    @JsonProperty("cloud_provider")
    public String cloudProvider;
    public String region;
    @JsonProperty("multi_az")
    public Boolean multiAZ;
}