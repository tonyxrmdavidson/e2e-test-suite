package io.managed.services.test.client.securitymgmt;

import com.openshift.cloud.api.kas.SecurityApi;
import com.openshift.cloud.api.kas.invoker.ApiClient;
import com.openshift.cloud.api.kas.invoker.ApiException;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import com.openshift.cloud.api.kas.models.ServiceAccountList;
import com.openshift.cloud.api.kas.models.ServiceAccountRequest;
import io.managed.services.test.client.BaseApi;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiUnknownException;

public class SecurityMgmtApi extends BaseApi<ApiException> {

    private final SecurityApi api;

    public SecurityMgmtApi(ApiClient apiClient) {
        this(new SecurityApi(apiClient));
    }

    public SecurityMgmtApi(SecurityApi defaultApi) {
        super(ApiException.class);
        this.api = defaultApi;
    }

    @Override
    protected ApiUnknownException toApiException(ApiException e) {
        return new ApiUnknownException(e.getMessage(), e.getCode(), e.getResponseHeaders(), e.getResponseBody(), e);
    }

    @SuppressWarnings("unused")
    public ServiceAccount getServiceAccountById(String id) throws ApiGenericException {
        return retry(() -> api.getServiceAccountById(id));
    }

    public ServiceAccountList getServiceAccounts() throws ApiGenericException {
        return retry(() -> api.getServiceAccounts());
    }

    public ServiceAccount createServiceAccount(ServiceAccountRequest serviceAccountRequest) throws ApiGenericException {
        return retry(() -> api.createServiceAccount(serviceAccountRequest));
    }

    public void deleteServiceAccountById(String id) throws ApiGenericException {
        // TODO: why does it return Error
        retry(() -> api.deleteServiceAccountById(id));
    }

    public ServiceAccount resetServiceAccountCreds(String id) throws ApiGenericException {
        return retry(() -> api.resetServiceAccountCreds(id));
    }
}
