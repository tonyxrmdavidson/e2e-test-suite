package io.managed.services.test.operator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ServiceBindingStatus {
    public List<ServiceBindingApplication> applications;
    public List<ServiceBindingCondition> conditions = new ArrayList<>();
    public String secret;
}
