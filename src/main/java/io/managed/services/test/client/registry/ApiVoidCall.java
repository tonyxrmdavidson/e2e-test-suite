package io.managed.services.test.client.registry;


import com.openshift.cloud.api.srs.invoker.ApiException;

@FunctionalInterface
public interface ApiVoidCall {
    void call() throws ApiException;
}
