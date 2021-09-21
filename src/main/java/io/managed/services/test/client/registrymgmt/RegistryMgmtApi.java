package io.managed.services.test.client.registrymgmt;

import com.openshift.cloud.api.srs.RegistriesApi;
import com.openshift.cloud.api.srs.invoker.ApiClient;
import com.openshift.cloud.api.srs.invoker.ApiException;
import com.openshift.cloud.api.srs.models.RegistryCreateRest;
import com.openshift.cloud.api.srs.models.RegistryListRest;
import com.openshift.cloud.api.srs.models.RegistryRest;
import io.managed.services.test.client.BaseApi;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiUnknownException;

public class RegistryMgmtApi extends BaseApi<ApiException> {

    private final RegistriesApi registriesApi;

    public RegistryMgmtApi(ApiClient apiClient) {
        this(new RegistriesApi(apiClient));
    }

    public RegistryMgmtApi(RegistriesApi registriesApi) {
        super(ApiException.class);
        this.registriesApi = registriesApi;
    }

    @Override
    protected ApiUnknownException toApiException(ApiException e) {
        return new ApiUnknownException(e.getMessage(), e.getCode(), e.getResponseHeaders(), e.getResponseBody(), e);
    }

    public RegistryRest createRegistry(RegistryCreateRest registryCreateRest) throws ApiGenericException {
        return retry(() -> registriesApi.createRegistry(registryCreateRest));
    }

    public RegistryRest getRegistry(String id) throws ApiGenericException {
        return retry(() -> registriesApi.getRegistry(id));
    }

    public RegistryListRest getRegistries(Integer page, Integer size, String orderBy, String search) throws ApiGenericException {
        return retry(() -> registriesApi.getRegistries(page, size, orderBy, search));
    }

    public void deleteRegistry(String id) throws ApiGenericException {
        retry(() -> registriesApi.deleteRegistry(id));
    }
}
