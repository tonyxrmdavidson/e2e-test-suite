package io.managed.services.test.client.registry;

import com.openshift.cloud.api.srs.invoker.ApiClient;
import com.openshift.cloud.api.srs.models.RegistryCreateRest;
import com.openshift.cloud.api.srs.models.RegistryListRest;
import com.openshift.cloud.api.srs.models.RegistryRest;
import io.managed.services.test.client.exception.ApiException;

public class RegistriesApi {

    private final com.openshift.cloud.api.srs.RegistriesApi registriesApi;

    public RegistriesApi(ApiClient apiClient) {
        this.registriesApi = new com.openshift.cloud.api.srs.RegistriesApi(apiClient);
    }

    public RegistriesApi(com.openshift.cloud.api.srs.RegistriesApi registriesApi) {
        this.registriesApi = registriesApi;
    }

    private static <T> T handle(ApiCall<T> f) throws ApiException {
        try {
            return f.call();
        } catch (com.openshift.cloud.api.srs.invoker.ApiException e) {
            throw ApiException.apiException(e);
        }
    }

    private static void vhandle(ApiVoidCall f) throws ApiException {
        try {
            f.call();
        } catch (com.openshift.cloud.api.srs.invoker.ApiException e) {
            throw ApiException.apiException(e);
        }
    }

    public RegistryRest createRegistry(RegistryCreateRest registryCreateRest) throws ApiException {
        return handle(() -> registriesApi.createRegistry(registryCreateRest));
    }

    public RegistryRest getRegistry(String id) throws ApiException {
        return handle(() -> registriesApi.getRegistry(id));
    }

    public RegistryListRest getRegistries(Integer page, Integer size, String orderBy, String search) throws ApiException {
        return handle(() -> registriesApi.getRegistries(page, size, orderBy, search));
    }

    public void deleteRegistry(String id) throws ApiException {
        vhandle(() -> registriesApi.deleteRegistry(id));
    }
}
