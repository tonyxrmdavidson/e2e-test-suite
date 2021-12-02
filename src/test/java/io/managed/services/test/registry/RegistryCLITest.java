package io.managed.services.test.registry;

import io.managed.services.test.Environment;
import io.managed.services.test.cli.CLI;
import io.managed.services.test.cli.CLIDownloader;
import io.managed.services.test.cli.CLIUtils;
import io.managed.services.test.cli.CliGenericException;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.devexp.KafkaCLITest;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;

import static io.managed.services.test.TestUtils.bwait;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

@Test
public class RegistryCLITest {
    private static final Logger LOGGER = LogManager.getLogger(RegistryCLITest.class);

    private static final String SERVICE_REGISTRY_NAME = "cli-e2e-test-registry-" + Environment.LAUNCH_KEY;

    private final Vertx vertx = Vertx.vertx();

    private CLI cli;

    @BeforeClass
    public void bootstrap() throws Throwable {

        assertNotNull(Environment.PRIMARY_USERNAME, "the PRIMARY_USERNAME env is null");
        assertNotNull(Environment.PRIMARY_PASSWORD, "the PRIMARY_PASSWORD env is null");

        LOGGER.info("download cli");
        var downloader = CLIDownloader.defaultDownloader();
        var binary = downloader.downloadCLIInTempDir();
        this.cli = new CLI(binary);

        LOGGER.info("login to RHOAS");
        CLIUtils.login(vertx, cli, Environment.PRIMARY_USERNAME, Environment.PRIMARY_PASSWORD).get();

    }

    @AfterClass(alwaysRun = true)
    @SneakyThrows
    public void clean() {

        try {
            LOGGER.info("logout user from rhoas");
            cli.logout();
        } catch (Throwable t) {
            LOGGER.error("logoutCLI error: ", t);
        }

        try {
            LOGGER.info("delete workdir: {}", cli.getWorkdir());
            FileUtils.deleteDirectory(new File(cli.getWorkdir()));
        } catch (Throwable t) {
            LOGGER.error("cleanWorkdir error: ", t);
        }

        bwait(vertx.close());
    }

    @Test
    public void testLogin(){

        LOGGER.info("Hello test");

    }

    @Test(enabled=false)
    @SneakyThrows
    public void testDescribeKafkaInstance() {

        LOGGER.info("get kafka instance with name {}", "KAFKA_INSTANCE_NAME");
        var k = cli.describeKafka("id");
        LOGGER.debug(k);

        assertEquals("ready", k.getStatus());
    }

    @Test(enabled=false)
    @SneakyThrows
    public void testListKafkaInstances() {

        var list = cli.listKafka();
        LOGGER.debug(list);

        var exists = list.getItems().stream()
                .filter(k -> "lol".equals(k.getName()))
                .findAny();
        assertTrue(exists.isPresent());
    }

    @Test
    @SneakyThrows
    public void testCreateServiceRegistry() {

        LOGGER.info("create service registry with name {}", SERVICE_REGISTRY_NAME);
        var r = cli.createServiceRegistry(SERVICE_REGISTRY_NAME);
        LOGGER.debug(r);

        LOGGER.info("wait for service registry with name: {}, with id: {}", r.getName(), r.getId());
        var registry = CLIUtils.waitUntilServiceRegistryIsReady(cli, r.getName());
        LOGGER.debug(registry);
    }

//TODO testCreateServiceRegistry:
// Description: Create and wait for the service registry using the CLI


// TODO  testDescribeServiceRegistry:
//  Description: Retrieve the previously created service registry by id


// TODO testListServiceRegistry:
//  Description: Retrieve all service registries and assert that the previously created registry is in the list

// (optional) TODO testDescribeServiceRegistryWithoutIdShouldFail

// TODO testUseServiceRegistry:
//  Description: Use the previously created registry by id, then describe the currently active registry (without using the id)


// TODO testDeleteServiceRegistry:
//  Description: Delete the previously created registry by id and then try to retrieve it and assert that it fails
}
