package io.managed.services.test.client.github;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.vertx.core.json.Json;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Release {
    public String id;
    @JsonProperty("tag_name")
    public String tagName;
    public String name;
    public List<Asset> assets;

    @Override
    public String toString() {
        return Json.encode(this);
    }
}
