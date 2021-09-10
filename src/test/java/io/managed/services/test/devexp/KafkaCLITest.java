package io.managed.services.test.devexp;

import com.openshift.cloud.api.kas.auth.models.Topic;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.ServiceAccountListItem;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.cli.CLI;
import io.managed.services.test.cli.CLIDownloader;
import io.managed.services.test.cli.CLIUtils;
import io.managed.services.test.cli.CliGenericException;
import io.managed.services.test.cli.CliNotFoundException;
import io.managed.services.test.cli.ServiceAccountSecret;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Objects;

import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/**
 * Test the application services CLI[1] kafka commands.
 * <p>
 * The tests download the CLI from GitHub to the local machine where the test suite is running
 * and perform all operations using the CLI.
 * <p>
 * By default the latest version of the CLI is downloaded otherwise a specific version can be set using
 * the CLI_VERSION env. The CLI platform (linux, mac, win) and arch (amd64, arm) is automatically detected,
 * or it can be enforced using the CLI_PLATFORM and CLI_ARCH env.
 * <p>
 * The SSO_USERNAME and SSO_PASSWORD will be used to login to the service through the CLI.
 * <p>
 * The tested environment is given by the SERVICE_API_URI and MAS_SSO_REDHAT_KEYCLOAK_URI env.
 * <p>
 * 1. https://github.com/redhat-developer/app-services-cli
 */
@Test(groups = TestTag.CLI)
public class KafkaCLITest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(KafkaCLITest.class);

    private static final String KAFKA_INSTANCE_NAME = "cli-e2e-test-instance-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String SERVICE_ACCOUNT_NAME = "cli-e2e-service-account-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String TOPIC_NAME = "cli-e2e-test-topic";
    private static final int DEFAULT_PARTITIONS = 1;
    private static final String CONSUMER_GROUP_NAME = "consumer-group-1";

    private final Vertx vertx = Vertx.vertx();

    private CLI cli;


    private KafkaRequest kafka;
    private ServiceAccountSecret serviceAccountSecret;
    private ServiceAccountListItem serviceAccount;
    private Topic topic;

    @AfterClass(timeOut = DEFAULT_TIMEOUT, alwaysRun = true)
    @SneakyThrows
    public void clean() {

        var apis = ApplicationServicesApi.applicationServicesApi(Environment.SERVICE_API_URI,
            Environment.SSO_USERNAME, Environment.SSO_PASSWORD);

        try {
            KafkaMgmtApiUtils.deleteKafkaByNameIfExists(apis.kafkaMgmt(), KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            LOGGER.error("delete kafka instance error: ", t);
        }

        try {
            SecurityMgmtAPIUtils.deleteServiceAccountByNameIfExists(apis.securityMgmt(), SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            LOGGER.error("delete service account error: ", t);
        }

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

    @Test(timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testDownloadCLI() {

        var downloader = CLIDownloader.defaultDownloader(vertx);

        // download the cli
        var binary = bwait(downloader.downloadCLIInTempDir());

        this.cli = new CLI(binary.directory, binary.name);

        LOGGER.info("validate cli");
        LOGGER.debug(cli.help());
    }


    @Test(dependsOnMethods = "testDownloadCLI", timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testLogin() {

        LOGGER.info("verify that we aren't logged-in");
        assertThrows(CliGenericException.class, () -> cli.listKafka());

        LOGGER.info("login the CLI");
        CLIUtils.login(vertx, cli, Environment.SSO_USERNAME, Environment.SSO_PASSWORD).get();

        LOGGER.info("verify that we are logged-in");
        cli.listKafka();
    }

    @Test(dependsOnMethods = "testLogin", timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testCreateServiceAccount() {

        LOGGER.info("create a service account");
        serviceAccountSecret = CLIUtils.createServiceAccount(cli, SERVICE_ACCOUNT_NAME);

        LOGGER.info("get the service account");
        var sa = CLIUtils.getServiceAccountByName(cli, SERVICE_ACCOUNT_NAME);
        LOGGER.debug(sa);

        assertTrue(sa.isPresent());
        assertEquals(sa.get().getName(), SERVICE_ACCOUNT_NAME);
        assertEquals(sa.get().getClientId(), serviceAccountSecret.getClientID());

        serviceAccount = sa.get();
    }

    @Test(dependsOnMethods = "testCreateServiceAccount", timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testDescribeServiceAccount() {

        var sa = cli.describeServiceAccount(serviceAccount.getId());
        LOGGER.debug(sa);

        assertEquals(sa.getName(), SERVICE_ACCOUNT_NAME);
    }

    @Test(dependsOnMethods = "testLogin", timeOut = 15 * MINUTES)
    @SneakyThrows
    public void testCreateKafkaInstance() {

        LOGGER.info("create kafka instance with name {}", KAFKA_INSTANCE_NAME);
        var k = cli.createKafka(KAFKA_INSTANCE_NAME);
        LOGGER.debug(k);

        LOGGER.info("wait for kafka instance: {}", k.getId());
        kafka = CLIUtils.waitUntilKafkaIsReady(cli, k.getId());
        LOGGER.debug(kafka);
    }

    @Test(dependsOnMethods = "testCreateKafkaInstance", timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testDescribeKafkaInstance() {

        LOGGER.info("get kafka instance with name {}", KAFKA_INSTANCE_NAME);
        var k = cli.describeKafka(kafka.getId());
        LOGGER.debug(k);

        assertEquals("ready", k.getStatus());
    }

    @Test(dependsOnMethods = "testCreateKafkaInstance", timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testListKafkaInstances() {

        var list = cli.listKafka();
        LOGGER.debug(list);

        var exists = list.getItems().stream()
            .filter(k -> KAFKA_INSTANCE_NAME.equals(k.getName()))
            .findAny();
        assertTrue(exists.isPresent());
    }

    @Test(dependsOnMethods = "testCreateKafkaInstance", timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testSearchKafkaByName() {

        var list = cli.searchKafkaByName(KAFKA_INSTANCE_NAME);
        LOGGER.debug(list);

        var exists = list.getItems().stream().findAny();
        assertTrue(exists.isPresent());
        assertEquals(exists.get().getName(), KAFKA_INSTANCE_NAME);
    }

    @Test(dependsOnMethods = "testCreateKafkaInstance", timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testCreateTopic() {

        LOGGER.info("create kafka topic with name {}", KAFKA_INSTANCE_NAME);
        topic = cli.createTopic(TOPIC_NAME);
        LOGGER.debug(topic);

        assertEquals(topic.getName(), TOPIC_NAME);
        assertEquals(Objects.requireNonNull(topic.getPartitions()).size(), DEFAULT_PARTITIONS);
    }

    @Test(dependsOnMethods = "testCreateTopic", timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testListTopics() {

        var list = cli.listTopics();
        LOGGER.debug(list);

        var exists = Objects.requireNonNull(list.getItems()).stream()
            .filter(t -> TOPIC_NAME.equals(t.getName()))
            .findAny();
        assertTrue(exists.isPresent());
    }


    @Test(dependsOnMethods = "testCreateTopic", timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testKafkaInstanceTopic() {

        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccountSecret.getClientID();
        var clientSecret = serviceAccountSecret.getClientSecret();

        bwait(testTopic(
            vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            TOPIC_NAME,
            1000,
            10,
            100));
    }

    @Test(dependsOnMethods = "testCreateTopic", timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testUpdateTopic() {

        var retentionTime = "4";
        var retentionKey = "retention.ms";

        LOGGER.info("update kafka topic with name {}", TOPIC_NAME);
        var t = cli.updateTopic(TOPIC_NAME, retentionTime);
        LOGGER.debug(t);

        var retentionValue = Objects.requireNonNull(t.getConfig())
            .stream()
            .filter(conf -> retentionKey.equals(conf.getKey()))
            .findFirst();

        assertTrue(retentionValue.isPresent(), "updated config not found");
        assertEquals(retentionValue.get().getValue(), retentionTime);

        topic = t;
    }

    @Test(dependsOnMethods = "testUpdateTopic", timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testDescribeUpdatedTopic() {

        var retentionTime = "4";
        var retentionKey = "retention.ms";

        LOGGER.info("describe kafka topic with name {}", TOPIC_NAME);
        var t = cli.describeTopic(TOPIC_NAME);
        LOGGER.debug(t);

        assertEquals(t.getName(), TOPIC_NAME);
        assertEquals(
            Objects.requireNonNull(t.getPartitions()).size(),
            Objects.requireNonNull(topic.getPartitions()).size());

        var retentionValue = Objects.requireNonNull(t.getConfig())
            .stream()
            .filter(conf -> retentionKey.equals(conf.getKey()))
            .findFirst();

        assertTrue(retentionValue.isPresent(), "updated config not found");
        assertEquals(retentionValue.get().getValue(), retentionTime);
    }

    @Test(dependsOnMethods = "testUpdateTopic", timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testKafkaInstanceUpdatedTopic() {

        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccountSecret.getClientID();
        var clientSecret = serviceAccountSecret.getClientSecret();

        bwait(testTopic(
            vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            TOPIC_NAME,
            1000,
            10,
            100));
    }

    @Test(dependsOnMethods = "testCreateTopic", timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testDescribeConsumerGroup() {

        var consumer = bwait(KafkaInstanceApiUtils.startConsumerGroup(vertx,
            CONSUMER_GROUP_NAME,
            TOPIC_NAME,
            kafka.getBootstrapServerHost(),
            serviceAccountSecret.getClientID(),
            serviceAccountSecret.getClientSecret()));
        bwait(consumer.close());

        var group = CLIUtils.waitForConsumerGroup(cli, CONSUMER_GROUP_NAME);
        LOGGER.debug(group);

        assertEquals(group.getGroupId(), CONSUMER_GROUP_NAME);
    }

    @Test(dependsOnMethods = "testDescribeConsumerGroup", timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testListConsumerGroups() {
        var groups = cli.listConsumerGroups();
        LOGGER.debug(groups);

        var filteredGroup = Objects.requireNonNull(groups.getItems())
            .stream()
            .filter(g -> CONSUMER_GROUP_NAME.equals(g.getGroupId()))
            .findAny();

        assertTrue(filteredGroup.isPresent());
    }

    @Test(dependsOnMethods = "testDescribeConsumerGroup", priority = 1, timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testDeleteConsumerGroup() {

        LOGGER.info("delete consumer group '{}'", CONSUMER_GROUP_NAME);
        cli.deleteConsumerGroup(CONSUMER_GROUP_NAME);

        assertThrows(CliNotFoundException.class,
            () -> cli.describeConsumerGroup(CONSUMER_GROUP_NAME));
    }

    @Test(dependsOnMethods = "testCreateTopic", priority = 2, timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testDeleteTopic() {

        LOGGER.info("delete topic '{}'", TOPIC_NAME);
        cli.deleteTopic(TOPIC_NAME);

        assertThrows(CliNotFoundException.class,
            () -> cli.describeTopic(TOPIC_NAME));
    }

    @Test(dependsOnMethods = "testCreateServiceAccount", priority = 2, timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testDeleteServiceAccount() {

        LOGGER.info("delete service account '{}'", serviceAccount.getId());
        cli.deleteServiceAccount(serviceAccount.getId());

        assertThrows(CliNotFoundException.class,
            () -> cli.describeServiceAccount(serviceAccount.getId()));
    }

    @Test(dependsOnMethods = "testCreateKafkaInstance", priority = 3, timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testDeleteKafkaInstance() {

        LOGGER.info("delete kafka instance '{}'", kafka.getId());
        cli.deleteKafka(kafka.getId());

        CLIUtils.waitUntilKafkaIsDeleted(cli, kafka.getId());
    }

    @Test(dependsOnMethods = "testLogin", priority = 3, timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testLogout() {

        LOGGER.info("verify that we are logged-in");
        cli.listKafka(); // successfully run cli command while logged in

        LOGGER.info("logout");
        cli.logout();

        LOGGER.info("verify that we are logged-in");
        assertThrows(CliGenericException.class, () -> cli.listKafka()); // unable to run the same command after logout
    }
}
