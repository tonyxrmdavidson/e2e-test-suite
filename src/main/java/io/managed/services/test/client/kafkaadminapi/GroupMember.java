package io.managed.services.test.client.kafkaadminapi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@Deprecated
@JsonIgnoreProperties(ignoreUnknown = true)
public class GroupMember {
    public String clientId;
    public String consumerId;
    public List<Integer> assignment;
    public String host;
}
