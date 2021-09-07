package io.managed.services.test.client.serviceapi;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Deprecated
@JsonIgnoreProperties(ignoreUnknown = true)
public class ServiceAccount {
    public String description;
    public String href;
    public String id;
    public String server;
    public String kind;
    @JsonProperty("client_id")
    @JsonAlias({"clientID"})
    public String clientID;
    @JsonProperty("client_secret")
    public String clientSecret;
    public String name;
    public String owner;
    @JsonProperty("created_at")
    public String createdAt;

}
