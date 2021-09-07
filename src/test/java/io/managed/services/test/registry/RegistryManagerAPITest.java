package io.managed.services.test.registry;

import com.openshift.cloud.api.srs.models.RegistryCreateRest;
import com.openshift.cloud.api.srs.models.RegistryRest;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.registrymgmt.RegistryMgmtApi;
import io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.message;
import static io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils.cleanRegistry;
import static io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils.registryMgmtApi;
import static io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils.waitUntilRegistryIsReady;
import static io.managed.services.test.client.registry.RegistryClientUtils.registryClient;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

@Test(groups = TestTag.REGISTRY)
public class RegistryManagerAPITest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(RegistryManagerAPITest.class);

    private static final String SERVICE_REGISTRY_NAME = "mk-e2e-sr-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String SERVICE_REGISTRY_2_NAME = "mk-e2e-sr2-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String ARTIFACT_SCHEMA = "{\"type\":\"record\",\"name\":\"Greeting\",\"fields\":[{\"name\":\"Message\",\"type\":\"string\"},{\"name\":\"Time\",\"type\":\"long\"}]}";

    private RegistryMgmtApi registryMgmtApi;
    private RegistryRest registry;

    @BeforeClass
    public void bootstrap() throws Throwable {
        registryMgmtApi = bwait(RegistryMgmtApiUtils.registryMgmtApi(Vertx.vertx()));
    }

    @AfterClass(timeOut = DEFAULT_TIMEOUT, alwaysRun = true)
    public void teardown() {
        assumeTeardown();

        try {
            cleanRegistry(registryMgmtApi, SERVICE_REGISTRY_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean service registry error: ", t);
        }

        try {
            cleanRegistry(registryMgmtApi, SERVICE_REGISTRY_2_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean service registry error: ", t);
        }
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testCreateRegistry() throws Exception {

        var registryCreateRest = new RegistryCreateRest()
            .name(SERVICE_REGISTRY_NAME)
            .description("Hello World!");

        var registry = registryMgmtApi.createRegistry(registryCreateRest);
        LOGGER.info("service registry: {}", Json.encode(registry));

        registry = waitUntilRegistryIsReady(registryMgmtApi, registry.getId());
        LOGGER.info("ready service registry: {}", Json.encode(registry));

        assertNotNull(registry.getRegistryUrl());

        this.registry = registry;
    }

    @Test(dependsOnMethods = "testCreateRegistry", timeOut = DEFAULT_TIMEOUT)
    public void testCreateArtifact() throws Throwable {
        var registryClient = bwait(registryClient(Vertx.vertx(), registry.getRegistryUrl()));

        LOGGER.info("create artifact on registry");
        var artifactMetaData = registryClient.createArtifact(null, null, IOUtils.toInputStream(ARTIFACT_SCHEMA, StandardCharsets.UTF_8));

        assertEquals(artifactMetaData.getName(), "Greeting");
    }

    @Test(dependsOnMethods = "testCreateRegistry", timeOut = DEFAULT_TIMEOUT)
    public void testListRegistries() throws ApiGenericException {

        // List registries
        var registries = registryMgmtApi.getRegistries(null, null, null, null);

        assertTrue(registries.getItems().size() > 0, "registries list is empty");

        var found = registries.getItems().stream()
            .anyMatch(r -> SERVICE_REGISTRY_NAME.equals(r.getName()));
        assertTrue(found, message("{} not found in registries list: {}", SERVICE_REGISTRY_NAME, Json.encode(registries)));
    }

    @Test(dependsOnMethods = "testCreateRegistry", timeOut = DEFAULT_TIMEOUT)
    public void testSearchRegistry() throws ApiGenericException {

        // Search registry by name
        var registries = registryMgmtApi.getRegistries(null, null, null,
            String.format("name = %s", SERVICE_REGISTRY_NAME));

        assertTrue(registries.getItems().size() > 0, "registries list is empty");
        assertTrue(registries.getItems().size() < 2, message("registries list contains more than one result: {}", Json.encode(registries)));
        assertEquals(registries.getItems().get(0).getName(), SERVICE_REGISTRY_NAME);
    }

    @Test(dependsOnMethods = "testCreateRegistry", timeOut = DEFAULT_TIMEOUT, enabled = false)
    public void testFailToCreateRegistryIfItAlreadyExist() {
        // TODO: Enable after https://github.com/bf2fc6cc711aee1a0c2a/srs-fleet-manager/issues/75

        var registryCreateRest = new RegistryCreateRest()
            .name(SERVICE_REGISTRY_NAME);

        assertThrows(() -> registryMgmtApi.createRegistry(registryCreateRest));
    }

    @Test(timeOut = DEFAULT_TIMEOUT, priority = 1, dependsOnMethods = "testCreateRegistry")
    public void testDeleteRegistry() throws Throwable {

        LOGGER.info("delete registry '{}'", registry.getId());
        registryMgmtApi.deleteRegistry(registry.getId());

        LOGGER.info("verify the registry '{}' has been deleted", registry.getId());
        RegistryMgmtApiUtils.waitUntilRegistryIsDeleted(registryMgmtApi, registry.getId());
    }

    @Test(priority = 2, timeOut = DEFAULT_TIMEOUT)
     public void testDeleteProvisioningRegistry() throws Throwable {

        var registryCreateRest = new RegistryCreateRest()
            .name(SERVICE_REGISTRY_NAME);

        LOGGER.info("create kafka instance: {}", SERVICE_REGISTRY_2_NAME);
        var registryToDelete = registryMgmtApi.createRegistry(registryCreateRest);

        LOGGER.info("delete the registry: {}", registryToDelete.getId());
        registryMgmtApi.deleteRegistry(registryToDelete.getId());

        LOGGER.info("verify the registry '{}' has been deleted", registryToDelete.getId());
        RegistryMgmtApiUtils.waitUntilRegistryIsDeleted(registryMgmtApi, registryToDelete.getId());
    }
}
