package io.managed.services.test.registry;

import com.openshift.cloud.api.srs.invoker.Configuration;
import com.openshift.cloud.api.srs.invoker.auth.HttpBearerAuth;
import com.openshift.cloud.api.srs.models.RegistryCreateRest;
import com.openshift.cloud.api.srs.models.RegistryRest;
import com.openshift.cloud.api.srs.models.RegistryStatusValueRest;
import io.managed.services.test.BooleanFunction;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.exception.ApiNotFoundException;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.managed.services.test.client.registry.RegistriesApi;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicReference;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.waitFor;
import static io.managed.services.test.client.registry.RegistriesApiUtils.cleanRegistry;
import static java.time.Duration.ofSeconds;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

@Test(groups = TestTag.REGISTRY)
public class RegistryManagerAPITest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(RegistryManagerAPITest.class);

    static final String SERVICE_REGISTRY_NAME = "mk-e2e-sr-" + Environment.KAFKA_POSTFIX_NAME;

    private RegistriesApi registriesApi;
    private RegistryRest registry;

    @BeforeClass
    public void bootstrap() throws Throwable {
        var vertx = Vertx.vertx();
        var auth = new KeycloakOAuth(vertx);

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        var user = bwait(auth.login(
            Environment.SSO_REDHAT_KEYCLOAK_URI,
            Environment.SSO_REDHAT_REDIRECT_URI,
            Environment.SSO_REDHAT_REALM,
            Environment.SSO_REDHAT_CLIENT_ID,
            Environment.SSO_USERNAME,
            Environment.SSO_PASSWORD));

        var apiClient = Configuration.getDefaultApiClient();
        apiClient.setBasePath(Environment.SERVICE_API_URI);
        ((HttpBearerAuth) apiClient.getAuthentication("Bearer")).setBearerToken(KeycloakOAuth.getToken(user));

        registriesApi = new RegistriesApi(apiClient);
    }

    @AfterClass(timeOut = DEFAULT_TIMEOUT, alwaysRun = true)
    public void teardown() {
        assumeTeardown();

        try {
            cleanRegistry(registriesApi, SERVICE_REGISTRY_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean service registry error: ", t);
        }
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testCreateRegistry() throws Throwable {

        var registryCreateRest = new RegistryCreateRest()
            .name(SERVICE_REGISTRY_NAME)
            .description("Hello World!");

        var registry = registriesApi.createRegistry(registryCreateRest);
        LOGGER.info("service registry: {}", Json.encode(registry));

        var registryReference = new AtomicReference<RegistryRest>();
        BooleanFunction isReady = last -> {
            var r = registriesApi.getRegistry(registry.getId());

            if (last) {
                LOGGER.warn("last registry: {}", Json.encode(r));
            }

            if (RegistryStatusValueRest.READY.equals(r.getStatus())) {
                registryReference.set(r);
                return true;
            }
            return false;
        };
        waitFor("registry to be ready", ofSeconds(2), ofSeconds(10), isReady);

        var finalRegistry = registryReference.get();
        LOGGER.info("final service registry: {}", Json.encode(registryReference.get()));

        assertNotNull(finalRegistry.getRegistryUrl());

        this.registry = finalRegistry;
    }

    // TODO: Test create registry with the same name

    @Test(timeOut = DEFAULT_TIMEOUT, dependsOnMethods = "testCreateRegistry")
    public void testDeleteRegistry() throws Throwable {

        LOGGER.info("delete registry: {}", registry.getId());
        registriesApi.deleteRegistry(registry.getId());

        assertThrows(ApiNotFoundException.class, () -> registriesApi.getRegistry(registry.getId()));
    }
}
