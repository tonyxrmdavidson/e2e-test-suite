package io.managed.services.test;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.openqa.selenium.NotFoundException;

import java.util.function.Predicate;

import static io.managed.services.test.TestUtils.message;

public class JSONArray {

    private final JsonArray array;

    public JSONArray(JsonArray array) {
        this.array = array;
    }

    public JSONObject findObject(Predicate<JSONObject> condition) {
        var v = array.stream()
            .map(o -> new JSONObject((JsonObject) o))
            .filter(condition)
            .findFirst()
            .orElse(null);
        if (v == null) {
            throw new NotFoundException(message("failed to find object with custom predicate in array '{}'", Json.encode(array)));
        }
        return v;
    }

    public JSONObject findObject(String key, String value) {
        var v = array.stream()
            .map(o -> new JSONObject((JsonObject) o))
            .filter(o -> o.getString(key).equals(value))
            .findFirst()
            .orElse(null);
        if (v == null) {
            throw new NotFoundException(message("failed to find object with key '{}' and value '{}' in array '{}'", key, value, Json.encode(array)));
        }
        return v;
    }
}
