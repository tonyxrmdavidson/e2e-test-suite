package io.managed.services.test;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.openqa.selenium.NotFoundException;

import java.util.Map;

import static io.managed.services.test.TestUtils.message;

public class JSONObject {

    private final JsonObject object;

    public JSONObject(JsonObject object) {
        this.object = object;
    }

    public JSONObject(Map<String, Object> map) {
        this.object = new JsonObject(map);
    }

    public String getString(String key) {
        var v = object.getString(key, null);
        if (v == null) {
            throw new NotFoundException(message("failed to find key '{}' in object '{}'", key, Json.encode(object)));
        }
        return v;
    }

    public JSONObject getObject(String key) {
        var v = object.getJsonObject(key, null);
        if (v == null) {
            throw new NotFoundException(message("failed to find key '{}' in object '{}'", key, Json.encode(object)));
        }
        return new JSONObject(v);
    }

    public JSONArray getArray(String key) {
        var a = object.getJsonArray(key, null);
        if (a == null) {
            throw new NotFoundException(message("failed to find key '{}' in object '{}'", key, Json.encode(object)));
        }
        return new JSONArray(a);
    }

    public JSONObject put(String key, Object value) {
        object.put(key, value);
        return this;
    }
}
