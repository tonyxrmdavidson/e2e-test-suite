package io.managed.services.test.client.serviceapi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsumerGroupListResponse {
    public List<ConsumerGroupResponse> items;
}




