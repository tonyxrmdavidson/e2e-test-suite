package io.managed.services.test.client.registry;

import com.openshift.cloud.api.srs.models.RegistryRest;
import io.managed.services.test.client.exception.ApiException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class RegistriesApiUtils {
    private static final Logger LOGGER = LogManager.getLogger(RegistriesApiUtils.class);

    public static void cleanRegistry(RegistriesApi api, String name) throws ApiException {
        deleteRegistryByNameIfExists(api, name);
    }

    public static void deleteRegistryByNameIfExists(RegistriesApi api, String name) throws ApiException {

        // Attention: this delete all registries with the given name
        var registries = getRegistryByName(api, name);

        if (registries.isEmpty()) {
            LOGGER.warn("registry '{}' not found", name);
        }

        // TODO: refactor after the names are unique: https://github.com/bf2fc6cc711aee1a0c2a/srs-fleet-manager/issues/75
        for (var r : registries) {
            LOGGER.info("delete registry: {}", r.getId());
            api.deleteRegistry(r.getId());
        }
    }

    public static List<RegistryRest> getRegistryByName(RegistriesApi api, String name) throws ApiException {

        // TODO: remove workaround after https://github.com/bf2fc6cc711aee1a0c2a/srs-fleet-manager/issues/43

        // Attention: we support only 10 registries until the name doesn't became unique
        return api.getRegistries(1, 10, null, String.format("name=%s", name))
            .getItems();
    }
}
