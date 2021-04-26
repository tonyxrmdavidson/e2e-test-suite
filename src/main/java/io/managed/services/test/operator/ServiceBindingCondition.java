package io.managed.services.test.operator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ServiceBindingCondition {
    public String lastTransitionTime;
    public String message;
    public int observedGeneration;
    public String reason;
    public String status;
    public String type;
}
