package io.managed.services.test.registry;

import com.openshift.cloud.api.srs.invoker.Configuration;
import com.openshift.cloud.api.srs.invoker.auth.HttpBearerAuth;
import com.openshift.cloud.api.srs.models.RegistryCreateRest;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
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

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.client.registry.RegistriesApiUtils.cleanRegistry;

@Test(groups = TestTag.REGISTRY)
public class RegistryManagerAPITest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(RegistryManagerAPITest.class);

    static final String SERVICE_REGISTRY_NAME = "mk-e2e-sr-" + Environment.KAFKA_POSTFIX_NAME;

    private RegistriesApi registriesApi;

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
    public void testCreateServiceRegistry() throws Throwable {

        var registryCreateRest = new RegistryCreateRest()
            .name(SERVICE_REGISTRY_NAME)
            .description("Hello World!");

        var registryRest = registriesApi.createRegistry(registryCreateRest);
        LOGGER.info("service registry: {}", Json.encode(registryRest));
    }
}
