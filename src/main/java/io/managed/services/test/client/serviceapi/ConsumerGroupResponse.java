package io.managed.services.test.client.serviceapi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsumerGroupResponse {
    public List<Object> consumers;
    public String groupId;
}