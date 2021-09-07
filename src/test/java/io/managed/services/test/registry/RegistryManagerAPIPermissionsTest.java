package io.managed.services.test.registry;

import com.openshift.cloud.api.srs.models.RegistryRest;
import io.apicurio.registry.rest.client.exception.ForbiddenException;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.TestUtils;
import io.managed.services.test.client.exception.ApiForbiddenException;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiNotFoundException;
import io.managed.services.test.client.exception.ApiUnauthorizedException;
import io.managed.services.test.client.registrymgmt.RegistryMgmtApi;
import io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.ext.auth.User;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.client.registry.RegistryClientUtils.registryClient;
import static io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils.applyRegistry;
import static io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils.cleanRegistry;
import static io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils.registryMgmtApi;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

@Test(groups = TestTag.REGISTRY)
public class RegistryManagerAPIPermissionsTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(RegistryManagerAPIPermissionsTest.class);

    private static final String SERVICE_REGISTRY_NAME = "mk-e2e-sr-rmp-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String ARTIFACT_SCHEMA = "{\"type\":\"record\",\"name\":\"Greeting\",\"fields\":[{\"name\":\"Message\",\"type\":\"string\"},{\"name\":\"Time\",\"type\":\"long\"}]}";

    private final Vertx vertx = Vertx.vertx();

    private RegistryMgmtApi registryMgmtApi;
    private RegistryMgmtApi secondaryRegistryMgmtApi;
    private RegistryMgmtApi alienRegistryMgmtApi;

    private RegistryRest registry;

    @BeforeClass
    public void bootstrap() throws Throwable {
        registryMgmtApi = bwait(RegistryMgmtApiUtils.registryMgmtApi(
            Environment.SSO_USERNAME,
            Environment.SSO_PASSWORD));
        registry = applyRegistry(registryMgmtApi, SERVICE_REGISTRY_NAME);

        secondaryRegistryMgmtApi = bwait(RegistryMgmtApiUtils.registryMgmtApi(
            Environment.SSO_SECONDARY_USERNAME,
            Environment.SSO_SECONDARY_PASSWORD));

        alienRegistryMgmtApi = bwait(RegistryMgmtApiUtils.registryMgmtApi(
            Environment.SSO_ALIEN_USERNAME,
            Environment.SSO_ALIEN_PASSWORD));
    }

    @AfterClass(timeOut = DEFAULT_TIMEOUT, alwaysRun = true)
    public void teardown() throws Throwable {
        assumeTeardown();

        try {
            cleanRegistry(registryMgmtApi, SERVICE_REGISTRY_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean service registry error: ", t);
        }

        bwait(vertx.close());
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testSecondaryUserCanReadTheRegistry() throws ApiGenericException {
        var r = secondaryRegistryMgmtApi.getRegistry(registry.getId());
        assertEquals(r.getName(), registry.getName());
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testUserCanReadTheRegistry() throws ApiGenericException {
        LOGGER.info("registries: {}", Json.encode(registryMgmtApi.getRegistries(null, null, null, null)));
        LOGGER.info("registry: {}", Json.encode(registry));
        var r = registryMgmtApi.getRegistry(registry.getId());
        assertEquals(r.getName(), registry.getName());
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testAlienUserCanNotReadTheRegistry() {
        assertThrows(ApiNotFoundException.class, () -> alienRegistryMgmtApi.getRegistry(registry.getId()));
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testAlienUserCanNotCreateArtifactOnTheRegistry() throws Throwable {
        var registryClient = bwait(registryClient(vertx, registry.getRegistryUrl(),
            Environment.SSO_ALIEN_USERNAME,
            Environment.SSO_ALIEN_PASSWORD));

        assertThrows(ForbiddenException.class, () -> registryClient.createArtifact(null, null, IOUtils.toInputStream(ARTIFACT_SCHEMA, StandardCharsets.UTF_8)));
    }

    @Test(priority = 1, timeOut = DEFAULT_TIMEOUT)
    public void testSecondaryUserCanNotDeleteTheRegistry() {
        assertThrows(ApiForbiddenException.class, () -> secondaryRegistryMgmtApi.deleteRegistry(registry.getId()));
    }

    @Test(priority = 1, timeOut = DEFAULT_TIMEOUT)
    public void testAlienUserCanNotDeleteTheRegistry() {
        assertThrows(ApiForbiddenException.class, () -> alienRegistryMgmtApi.deleteRegistry(registry.getId()));
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testUnauthenticatedUserWithFakeToken() {
        var user = User.fromToken(TestUtils.FAKE_TOKEN);
        var api = registryMgmtApi(Environment.SERVICE_API_URI, user);
        assertThrows(ApiUnauthorizedException.class, () -> api.getRegistries(null, null, null, null));
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testUnauthenticatedUserWithoutToken() {
        var user = User.fromToken("");
        var api = registryMgmtApi(Environment.SERVICE_API_URI, user);
        assertThrows(ApiUnauthorizedException.class, () -> api.getRegistries(null, null, null, null));
    }
}
