/*
 * Kafka Service Fleet Manager
 * Kafka Service Fleet Manager is a Rest API to manage Kafka instances.
 *
 * The version of the OpenAPI document: 1.2.0
 *
 *
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */


package io.managed.services.test.client.securitymgmt.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.openshift.cloud.api.kas.models.Error;
import io.vertx.core.json.Json;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TemporaryError {
    private String error;

    @Override
    public String toString() {
        return Json.encode(this);
    }

    public Error toError() {
        return new Error()
            .reason(error);
    }
}

