package io.managed.services.test.observatorium;

import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

public class QueryResult {
    public String status;
    public Data data;

    public static class Data {
        public String resultType;
        public List<Result> result;
    }

    public static class Result {
        public Map<String, String> metric;
        public List<Object> value;
    }
}
