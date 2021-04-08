package io.managed.services.test.client.serviceapi;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ServiceAccount {
    public String description;
    public String href;
    public String id;
    public String server;
    public String kind;
    public String clientID;
    public String clientSecret;
    public String name;
    public String owner;
    @JsonProperty("created_at")
    public String createdAt;
}
