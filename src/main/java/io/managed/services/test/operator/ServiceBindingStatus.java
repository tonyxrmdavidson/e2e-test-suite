package io.managed.services.test.operator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.LinkedList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ServiceBindingStatus {
    public List<ServiceBindingApplication> applications;
    public List<ServiceBindingCondition> conditions = new LinkedList<>();
    public String secret;
}
