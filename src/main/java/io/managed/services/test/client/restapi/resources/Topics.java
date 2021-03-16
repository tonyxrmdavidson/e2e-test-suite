package io.managed.services.test.client.restapi.resources;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Topics {
    @JsonProperty("items")
    public List<Topic> topics;
    public int offset;
    public int limit;
    public int count;


}
