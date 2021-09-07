package io.managed.services.test.client.kafkaadminapi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Deprecated
@JsonIgnoreProperties(ignoreUnknown = true)
public class GroupCoordinator {
    public boolean hasRack;
    public String host;
    public int id;
    public int port;
    public Object rack;
    public boolean empty;
}
