package io.managed.services.test.client.github;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.vertx.core.json.Json;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Asset {
    public String id;
    public String name;
    public String label;

    @Override
    public String toString() {
        return Json.encode(this);
    }
}
