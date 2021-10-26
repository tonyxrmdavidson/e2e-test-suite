package io.managed.services.test.client;

import com.openshift.cloud.api.kas.invoker.ApiClient;
import com.openshift.cloud.api.kas.invoker.auth.HttpBearerAuth;

public class KasApiClient {
    private final ApiClient apiClient;

    public KasApiClient() {
        apiClient = new ApiClient();
    }

    public KasApiClient basePath(String basePath) {
        apiClient.setBasePath(basePath);
        return this;
    }

    public KasApiClient bearerToken(String token) {
        setBearerToken(token);
        return this;
    }

    public void setBearerToken(String token) {
        ((HttpBearerAuth) apiClient.getAuthentication("Bearer")).setBearerToken(token);
    }

    public ApiClient getApiClient() {
        return apiClient;
    }
}
