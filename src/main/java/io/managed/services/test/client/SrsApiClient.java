package io.managed.services.test.client;

import com.openshift.cloud.api.srs.invoker.ApiClient;
import com.openshift.cloud.api.srs.invoker.auth.HttpBearerAuth;

public class SrsApiClient {
    private final ApiClient apiClient;

    public SrsApiClient() {
        apiClient = new ApiClient();
    }

    public SrsApiClient basePath(String basePath) {
        apiClient.setBasePath(basePath);
        return this;
    }

    public SrsApiClient bearerToken(String token) {
        ((HttpBearerAuth) apiClient.getAuthentication("Bearer")).setBearerToken(token);
        return this;
    }

    public ApiClient getApiClient() {
        return apiClient;
    }
}
