package io.managed.services.test.client.registry;

import com.openshift.cloud.api.srs.invoker.ApiClient;
import com.openshift.cloud.api.srs.invoker.auth.HttpBearerAuth;
import com.openshift.cloud.api.srs.models.RegistryCreateRest;
import com.openshift.cloud.api.srs.models.RegistryListRest;
import com.openshift.cloud.api.srs.models.RegistryRest;
import com.openshift.cloud.api.srs.models.RegistryStatusValueRest;
import io.managed.services.test.Environment;
import io.managed.services.test.ThrowableFunction;
import io.managed.services.test.client.exception.ApiException;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeoutException;
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

            .map(user -> registriesApi(Environment.SERVICE_API_URI, KeycloakOAuth.getToken(user)));
    }

    public static RegistriesApi registriesApi(String uri, String token) {
        var apiClient = new ApiClient();
        apiClient.setBasePath(uri);
        ((HttpBearerAuth) apiClient.getAuthentication("Bearer")).setBearerToken(token);
        return new RegistriesApi(apiClient);
    }

    /**
     * Create a Registry using the default options if it doesn't exist or return the existing Registry
     *
     * @param api  RegistriesApi
     * @param name Name for the Registry
     * @return RegistryRest
     */
    public static RegistryRest applyRegistry(RegistriesApi api, String name)
        throws ApiException, InterruptedException, TimeoutException {

        var registryCreateRest = new RegistryCreateRest().name(name);
        return applyRegistry(api, registryCreateRest);
    }

    /**
     * Create a Registry if it doesn't exist or return the existing Registry
     *
     * @param api     RegistriesApi
     * @param payload RegistryCreateRest
     * @return RegistryRest
     */
    public static RegistryRest applyRegistry(RegistriesApi api, RegistryCreateRest payload)
        throws ApiException, InterruptedException, TimeoutException {

        var registryList = getRegistryByName(api, payload.getName());

        if (registryList.getItems().size() > 0) {
            var registry = registryList.getItems().get(0);
            LOGGER.warn("registry already exists: {}", Json.encode(registry));
            return registry;
        }

        LOGGER.info("create registry: {}", payload.getName());
        var registry = api.createRegistry(payload);

        registry = waitUntilRegistryIsReady(api, registry.getId());

        LOGGER.info("registry ready: {}", Json.encode(registry));
        return registry;
    }

    /**
     * Function that returns RegistryRest only if status is in ready
     *
     * @param api        RegistriesApi
     * @param registryID String
     * @return RegistryRest
     */
    public static RegistryRest waitUntilRegistryIsReady(RegistriesApi api, String registryID)
        throws InterruptedException, ApiException, TimeoutException {

        // save the last ready registry in the atomic reference
        var registryReference = new AtomicReference<RegistryRest>();

        ThrowableFunction<Boolean, Boolean, ApiException> isReady
            = last -> isRegistryReady(api.getRegistry(registryID), registryReference, last);

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

        if (registries.getItems().isEmpty()) {
            LOGGER.warn("registry '{}' not found", name);
        }

        // TODO: refactor after the names are unique: https://github.com/bf2fc6cc711aee1a0c2a/srs-fleet-manager/issues/75
        for (var r : registries.getItems()) {
            LOGGER.info("delete registry: {}", r.getId());
            api.deleteRegistry(r.getId());
        }
    }

    public static RegistryListRest getRegistryByName(RegistriesApi api, String name) throws ApiException {

        // Attention: we support only 10 registries until the name doesn't become unique
        return api.getRegistries(1, 10, null, String.format("name = %s", name));
    }
}
