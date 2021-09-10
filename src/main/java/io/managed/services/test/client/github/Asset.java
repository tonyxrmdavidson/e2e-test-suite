package io.managed.services.test.client.github;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.vertx.core.json.Json;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class Asset {
    private String id;
    private String name;
    private String label;

    @Override
    public String toString() {
        return Json.encode(this);
    }
}
