package io.managed.services.test.client.registry;

import com.openshift.cloud.api.srs.invoker.ApiClient;
import com.openshift.cloud.api.srs.invoker.ApiException;
import com.openshift.cloud.api.srs.models.RegistryCreateRest;
import com.openshift.cloud.api.srs.models.RegistryListRest;
import com.openshift.cloud.api.srs.models.RegistryRest;
import io.managed.services.test.client.BaseApi;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiUnknownException;

public class RegistriesApi extends BaseApi<ApiException> {

    private final com.openshift.cloud.api.srs.RegistriesApi registriesApi;

    public RegistriesApi(ApiClient apiClient) {
        this(new com.openshift.cloud.api.srs.RegistriesApi(apiClient));
    }

    public RegistriesApi(com.openshift.cloud.api.srs.RegistriesApi registriesApi) {
        super(ApiException.class);
        this.registriesApi = registriesApi;
    }

    @Override
    protected ApiUnknownException toApiException(ApiException e) {
        return new ApiUnknownException(e.getMessage(), e.getCode(), e.getResponseHeaders(), e.getResponseBody(), e);
    }

    public RegistryRest createRegistry(RegistryCreateRest registryCreateRest) throws ApiGenericException {
        return handle(() -> registriesApi.createRegistry(registryCreateRest));
    }

    public RegistryRest getRegistry(String id) throws ApiGenericException {
        return handle(() -> registriesApi.getRegistry(id));
    }

    public RegistryListRest getRegistries(Integer page, Integer size, String orderBy, String search) throws ApiGenericException {
        return handle(() -> registriesApi.getRegistries(page, size, orderBy, search));
    }

    public void deleteRegistry(String id) throws ApiGenericException {
        vhandle(() -> registriesApi.deleteRegistry(id));
    }
}
