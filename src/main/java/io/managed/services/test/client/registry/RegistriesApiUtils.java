package io.managed.services.test.client.registry;

import com.openshift.cloud.api.srs.invoker.Configuration;
import com.openshift.cloud.api.srs.invoker.auth.HttpBearerAuth;
import com.openshift.cloud.api.srs.models.RegistryRest;
import com.openshift.cloud.api.srs.models.RegistryStatusValueRest;
import io.managed.services.test.BooleanFunction;
import io.managed.services.test.Environment;
import io.managed.services.test.client.exception.ApiException;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static io.managed.services.test.TestUtils.waitFor;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;

public class RegistriesApiUtils {
    private static final Logger LOGGER = LogManager.getLogger(RegistriesApiUtils.class);

    public static Future<RegistriesApi> registriesApi(Vertx vertx) {
        return registriesApi(vertx, Environment.SSO_USERNAME, Environment.SSO_PASSWORD);
    }

    public static Future<RegistriesApi> registriesApi(Vertx vertx, String username, String password) {
        var auth = new KeycloakOAuth(vertx);

        LOGGER.info("authenticate user: {} against: {}", username, Environment.SSO_REDHAT_KEYCLOAK_URI);
        return auth.login(
                Environment.SSO_REDHAT_KEYCLOAK_URI,
                Environment.SSO_REDHAT_REDIRECT_URI,
                Environment.SSO_REDHAT_REALM,
                Environment.SSO_REDHAT_CLIENT_ID,
                username, password)

            .map(user -> {
                var apiClient = Configuration.getDefaultApiClient();
                apiClient.setBasePath(Environment.SERVICE_API_URI);
                ((HttpBearerAuth) apiClient.getAuthentication("Bearer")).setBearerToken(KeycloakOAuth.getToken(user));
                return new RegistriesApi(apiClient);
            });
    }

    /**
     * Function that returns RegistryRest only if status is in ready
     *
     * @param api        RegistriesApi
     * @param registryID String
     * @return RegistryRest
     */
    public static RegistryRest waitUntilRegistryIsReady(RegistriesApi api, String registryID) throws Exception {

        // save the last ready registry in the atomic reference
        var registryReference = new AtomicReference<RegistryRest>();

        BooleanFunction isReady = last -> isRegistryReady(api.getRegistry(registryID), registryReference, last);
        waitFor("registry to be ready", ofSeconds(3), ofMinutes(1), isReady);

        return registryReference.get();
    }

    public static boolean isRegistryReady(RegistryRest registry, AtomicReference<RegistryRest> reference, boolean last) {
        LOGGER.info("registry status is: {}", registry.getStatus());

        if (last) {
            LOGGER.warn("last registry response is: {}", Json.encode(registry));
        }

        reference.set(registry);

        return RegistryStatusValueRest.READY.equals(registry.getStatus());
    }


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
