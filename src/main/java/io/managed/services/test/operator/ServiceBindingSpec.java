package io.managed.services.test.operator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ServiceBindingSpec {
    public Application application;
    public boolean bindAsFiles;
    public List<Service> services;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Application {
        public String group;
        public String name;
        public String resource;
        public String version;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Service {
        public String group;
        public String version;
        public String kind;
        public String name;
    }
}
