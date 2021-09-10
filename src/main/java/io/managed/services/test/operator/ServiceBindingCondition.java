package io.managed.services.test.operator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class ServiceBindingCondition {
    private String lastTransitionTime;
    private String message;
    private int observedGeneration;
    private String reason;
    private String status;
    private String type;
}
