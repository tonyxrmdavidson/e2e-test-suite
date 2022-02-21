package io.managed.services.test.registry;

import com.openshift.cloud.api.srs.models.Registry;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.TestUtils;
import io.managed.services.test.client.exception.ApiForbiddenException;
import io.managed.services.test.client.exception.ApiNotFoundException;
import io.managed.services.test.client.exception.ApiUnauthorizedException;
import io.managed.services.test.client.oauth.KeycloakLoginSession;
import io.managed.services.test.client.oauth.KeycloakUser;
import io.managed.services.test.client.registry.RegistryClientUtils;
import io.managed.services.test.client.registrymgmt.RegistryMgmtApi;
import io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils;
import io.vertx.core.json.Json;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils.applyRegistry;
import static io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils.cleanRegistry;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

/**
 * Test the User authn and authz for the Registry Mgmt API.
 * <p>
 * <b>Requires:</b>
 * <ul>
 *     <li> PRIMARY_USERNAME
 *     <li> PRIMARY_PASSWORD
 *     <li> SECONDARY_USERNAME
 *     <li> SECONDARY_PASSWORD
 *     <li> ALIEN_USERNAME
 *     <li> ALIEN_PASSWORD
 *     <li> ADMIN_USERNAME
 *     <li> ADMIN_PASSWORD
 * </ul>
 */
public class RegistryMgmtAPIPermissionsTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(RegistryMgmtAPIPermissionsTest.class);

    private static final String SERVICE_REGISTRY_NAME = "mk-e2e-sr-rmp-" + Environment.LAUNCH_KEY;
    private static final String ARTIFACT_SCHEMA = "{\"type\":\"record\",\"name\":\"Greeting\",\"fields\":[{\"name\":\"Message\",\"type\":\"string\"},{\"name\":\"Time\",\"type\":\"long\"}]}";

    private KeycloakLoginSession adminAuth;
    private KeycloakLoginSession alienAuth;

    private RegistryMgmtApi primaryRegistryMgmtAPI;
    private RegistryMgmtApi secondaryRegistryMgmtAPI;
    private RegistryMgmtApi adminRegistryMgmtAPI;
    private RegistryMgmtApi alienRegistryMgmtAPI;

    private Registry registry;

    @BeforeClass
    public void bootstrap() throws Throwable {
        // initialize the auth objects for all users
        var primaryAuth = KeycloakLoginSession.primaryUser();
        var secondaryAuth = KeycloakLoginSession.secondaryUser();
        adminAuth = KeycloakLoginSession.adminUser();
        alienAuth = KeycloakLoginSession.alienUser();

        // initialize RedHat SSO users
        var primaryUser = primaryAuth.loginToRedHatSSO();
        var secondaryUser = secondaryAuth.loginToRedHatSSO();
        var adminUser = adminAuth.loginToRedHatSSO();
        var alienUser = alienAuth.loginToRedHatSSO();

        // initialize the Security mgmt APIs for all users
        primaryRegistryMgmtAPI = RegistryMgmtApiUtils.registryMgmtApi(primaryUser);
        secondaryRegistryMgmtAPI = RegistryMgmtApiUtils.registryMgmtApi(secondaryUser);
        adminRegistryMgmtAPI = RegistryMgmtApiUtils.registryMgmtApi(adminUser);
        alienRegistryMgmtAPI = RegistryMgmtApiUtils.registryMgmtApi(alienUser);

        registry = applyRegistry(primaryRegistryMgmtAPI, SERVICE_REGISTRY_NAME);
    }

    @AfterClass(alwaysRun = true)
    public void teardown() {
        assumeTeardown();

        try {
            if (primaryRegistryMgmtAPI != null) {
                cleanRegistry(primaryRegistryMgmtAPI, SERVICE_REGISTRY_NAME);
            }
        } catch (Throwable t) {
            LOGGER.error("clean service registry error: ", t);
        }
    }

    @Test
    @SneakyThrows
    public void testSecondaryUserCanReadTheRegistry() {
        var r = secondaryRegistryMgmtAPI.getRegistry(registry.getId());
        assertEquals(r.getName(), registry.getName());
    }

    @Test
    @SneakyThrows
    public void testUserCanReadTheRegistry() {
        LOGGER.info("registries: {}", Json.encode(primaryRegistryMgmtAPI.getRegistries(null, null, null, null)));
        LOGGER.info("registry: {}", Json.encode(registry));
        var r = primaryRegistryMgmtAPI.getRegistry(registry.getId());
        assertEquals(r.getName(), registry.getName());
    }

    @Test
    public void testAlienUserCanNotReadTheRegistry() {
        assertThrows(ApiNotFoundException.class, () -> alienRegistryMgmtAPI.getRegistry(registry.getId()));
    }

    @Test
    @SneakyThrows
    public void testAlienUserCanNotCreateArtifactOnTheRegistry() {
        var registryClient = RegistryClientUtils.registryClient(registry.getRegistryUrl(), alienAuth.loginToOpenshiftIdentity());
        assertThrows(ApiForbiddenException.class, () -> registryClient.createArtifact(null, null, ARTIFACT_SCHEMA.getBytes(StandardCharsets.UTF_8)));
    }

    @Test(priority = 1)
    public void testSecondaryUserCanNotDeleteTheRegistry() {
        assertThrows(ApiForbiddenException.class, () -> secondaryRegistryMgmtAPI.deleteRegistry(registry.getId()));
    }

    @Test(priority = 1)
    public void testAlienUserCanNotDeleteTheRegistry() {
        assertThrows(ApiForbiddenException.class, () -> alienRegistryMgmtAPI.deleteRegistry(registry.getId()));
    }

    @Test
    public void testUnauthenticatedUserWithFakeToken() {
        var api = RegistryMgmtApiUtils.registryMgmtApi(new KeycloakUser(TestUtils.FAKE_TOKEN));
        assertThrows(ApiUnauthorizedException.class, () -> api.getRegistries(null, null, null, null));
    }

    @Test
    public void testUnauthenticatedUserWithoutToken() {
        var api = RegistryMgmtApiUtils.registryMgmtApi(new KeycloakUser(""));
        assertThrows(ApiUnauthorizedException.class, () -> api.getRegistries(null, null, null, null));
    }

    @Test
    @SneakyThrows
    public void testAdminUserCanCreateArtifactOnTheRegistry() {
        var registryClient = RegistryClientUtils.registryClient(registry.getRegistryUrl(), adminAuth.loginToOpenshiftIdentity());
        registryClient.createArtifact(null, null, ARTIFACT_SCHEMA.getBytes(StandardCharsets.UTF_8));
    }

    @Test(priority = 2)
    @SneakyThrows
    public void testAdminUserCanDeleteTheRegistry() {
        // deletion of register by admin
        adminRegistryMgmtAPI.deleteRegistry(registry.getId());
    }
}
