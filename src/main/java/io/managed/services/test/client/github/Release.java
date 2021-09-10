package io.managed.services.test.client.github;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.vertx.core.json.Json;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Release {
    private String id;
    @JsonProperty("tag_name")
    private String tagName;
    private String name;
    private Boolean draft;
    @JsonProperty("prerelease")
    private Boolean preRelease;
    private List<Asset> assets;

    @Override
    public String toString() {
        return Json.encode(this);
    }
}
