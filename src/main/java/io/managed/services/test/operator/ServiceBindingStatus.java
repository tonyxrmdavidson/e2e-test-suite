package io.managed.services.test.operator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.LinkedList;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ServiceBindingStatus {
    private List<ServiceBindingApplication> applications = new LinkedList<>();
    private List<ServiceBindingCondition> conditions = new LinkedList<>();
    private String secret;
}
