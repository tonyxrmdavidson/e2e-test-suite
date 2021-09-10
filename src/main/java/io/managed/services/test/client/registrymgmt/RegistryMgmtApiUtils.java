package io.managed.services.test.client.registrymgmt;

import com.openshift.cloud.api.srs.models.RegistryCreateRest;
import com.openshift.cloud.api.srs.models.RegistryListRest;
import com.openshift.cloud.api.srs.models.RegistryRest;
import com.openshift.cloud.api.srs.models.RegistryStatusValueRest;
import io.managed.services.test.Environment;
import io.managed.services.test.ThrowableFunction;
import io.managed.services.test.client.SrsApiClient;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiNotFoundException;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.ext.auth.User;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.managed.services.test.TestUtils.waitFor;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;

public class RegistryMgmtApiUtils {
    private static final Logger LOGGER = LogManager.getLogger(RegistryMgmtApiUtils.class);

    public static Future<RegistryMgmtApi> registryMgmtApi(String username, String password) {
        return registryMgmtApi(new KeycloakOAuth(Vertx.vertx(), username, password));
    }

    public static Future<RegistryMgmtApi> registryMgmtApi(KeycloakOAuth auth) {
        LOGGER.info("authenticate user: {} against RH SSO", auth.getUsername());
        return auth.loginToRHSSO().map(u -> registryMgmtApi(u));
    }

    public static RegistryMgmtApi registryMgmtApi(User user) {
        return registryMgmtApi(Environment.SERVICE_API_URI, user);
    }

    public static RegistryMgmtApi registryMgmtApi(String uri, User user) {
        var token = KeycloakOAuth.getToken(user);
        return new RegistryMgmtApi(new SrsApiClient().basePath(uri).bearerToken(token).getApiClient());
    }

    /**
     * Create a Registry using the default options if it doesn't exist or return the existing Registry
     *
     * @param api  RegistryMgmtApi
     * @param name Name for the Registry
     * @return RegistryRest
     */
    public static RegistryRest applyRegistry(RegistryMgmtApi api, String name)
        throws ApiGenericException, InterruptedException, TimeoutException {

        var registryCreateRest = new RegistryCreateRest().name(name);
        return applyRegistry(api, registryCreateRest);
    }

    /**
     * Create a Registry if it doesn't exist or return the existing Registry
     *
     * @param api     RegistryMgmtApi
     * @param payload RegistryCreateRest
     * @return RegistryRest
     */
    public static RegistryRest applyRegistry(RegistryMgmtApi api, RegistryCreateRest payload)
        throws ApiGenericException, InterruptedException, TimeoutException {

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
     * @param api        RegistryMgmtApi
     * @param registryID String
     * @return RegistryRest
     */
    public static RegistryRest waitUntilRegistryIsReady(RegistryMgmtApi api, String registryID)
        throws InterruptedException, ApiGenericException, TimeoutException {

        // save the last ready registry in the atomic reference
        var registryReference = new AtomicReference<RegistryRest>();

        ThrowableFunction<Boolean, Boolean, ApiGenericException> isReady
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

    public static void cleanRegistry(RegistryMgmtApi api, String name) throws ApiGenericException {
        deleteRegistryByNameIfExists(api, name);
    }

    public static void deleteRegistryByNameIfExists(RegistryMgmtApi api, String name) throws ApiGenericException {

        // Attention: this deletes all registries with the given name
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

    // TODO: Move some of the most common method in the RegistryMgmtApi
    public static void waitUntilRegistryIsDeleted(RegistryMgmtApi api, String registryId)
        throws InterruptedException, ApiGenericException, TimeoutException {

        ThrowableFunction<Boolean, Boolean, ApiGenericException> isReady = last -> {
            try {
                var registry = api.getRegistry(registryId);
                LOGGER.debug(registry);
                return false;
            } catch (ApiNotFoundException e) {
                return true;
            }
        };

        waitFor("registry to be deleted", ofSeconds(1), ofSeconds(20), isReady);
    }

    public static RegistryListRest getRegistryByName(RegistryMgmtApi api, String name) throws ApiGenericException {

        // Attention: we support only 10 registries until the name doesn't become unique
        return api.getRegistries(1, 10, null, String.format("name = %s", name));
    }
}
