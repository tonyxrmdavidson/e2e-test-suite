package io.managed.services.test.operator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ServiceBindingApplication {
    public String group;
    public String kind;
    public String name;
    public String resource;
    public String version;
}
