package io.managed.services.test.client;

import com.openshift.cloud.api.kas.auth.invoker.ApiClient;
import com.openshift.cloud.api.kas.auth.invoker.auth.OAuth;

public class KasAuthApiClient {
    private final ApiClient apiClient;

    public KasAuthApiClient() {
        apiClient = new ApiClient();
    }

    public KasAuthApiClient basePath(String basePath) {
        apiClient.setBasePath(basePath);
        return this;
    }

    public KasAuthApiClient bearerToken(String token) {
        ((OAuth) apiClient.getAuthentication("Bearer")).setAccessToken(token);
        return this;
    }

    public ApiClient getApiClient() {
        return apiClient;
    }
}
