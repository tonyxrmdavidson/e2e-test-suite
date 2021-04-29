package io.managed.services.test;

import io.managed.services.test.cli.CLI;
import io.managed.services.test.cli.CLIDownloader;
import io.managed.services.test.cli.CLIUtils;
import io.managed.services.test.cli.ProcessException;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.client.serviceapi.ServiceAccount;
import io.managed.services.test.client.serviceapi.ServiceAccountSecret;
import io.managed.services.test.client.serviceapi.TopicResponse;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.cli.CLIUtils.waitForKafkaDelete;
import static io.managed.services.test.cli.CLIUtils.waitForKafkaReady;
import static io.managed.services.test.cli.CLIUtils.waitForTopicDelete;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopicWithOauth;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


@Tag(TestTag.CLI)
@Timeout(value = 5, unit = TimeUnit.MINUTES)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KafkaCLITest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(KafkaCLITest.class);

    static final String KAFKA_INSTANCE_NAME = "cli-e2e-test-instance-" + Environment.KAFKA_POSTFIX_NAME;
    static final String SERVICE_ACCOUNT_NAME = "cli-e2e-service-account-" + Environment.KAFKA_POSTFIX_NAME;
    static final String TOPIC_NAME = "cli-e2e-test-topic";
    static final int DEFAULT_PARTITIONS = 1;

    Vertx vertx = Vertx.vertx();
    CLI cli;
    boolean loggedIn;
    KafkaResponse kafkaInstance;
    ServiceAccount serviceAccount;
    TopicResponse topic;

    private Future<Void> cleanServiceAccount(ServiceAPI api) {
        LOGGER.info("delete service account with name: {}", SERVICE_ACCOUNT_NAME);
        return ServiceAPIUtils.deleteServiceAccountByNameIfExists(api, SERVICE_ACCOUNT_NAME);
    }

    private Future<Void> cleanKafkaInstance(ServiceAPI api) {
        LOGGER.info("delete kafka instance with name: {}", KAFKA_INSTANCE_NAME);
        return ServiceAPIUtils.deleteKafkaByNameIfExists(api, KAFKA_INSTANCE_NAME);
    }

    private void logoutCLI(CLI cli) {
        if (cli != null) {
            LOGGER.info("logout user from rhoas");
            cli.logout();
        }
    }

    private Future<Void> cleanWorkdir(CLI cli) {
        if (cli != null) {
            LOGGER.info("delete workdir: {}", cli.getWorkdir());
            return vertx.fileSystem().deleteRecursive(cli.getWorkdir(), true);
        }
        return Future.succeededFuture();
    }

    @AfterAll
    void clean() throws Throwable {

        var api = bwait(ServiceAPIUtils.serviceAPI(vertx));

        try {
            bwait(cleanServiceAccount(api));
        } catch (Throwable t) {
            LOGGER.error("cleanServiceAccount error: ", t);
        }

        try {
            bwait(cleanKafkaInstance(api));
        } catch (Throwable t) {
            LOGGER.error("cleanServiceAccount error: ", t);
        }

        try {
            logoutCLI(cli);
        } catch (Throwable t) {
            LOGGER.error("logoutCLI error: ", t);
        }

        try {
            bwait(cleanWorkdir(cli));
        } catch (Throwable t) {
            LOGGER.error("cleanWorkdir error: ", t);
        }
    }

    void assertCLI() {
        assumeTrue(cli != null, "cli is null because the bootstrap has failed");
    }

    void assertLoggedIn() {
        assumeTrue(loggedIn, "cli is not logged in");
    }

    void assertKafka() {
        assumeTrue(kafkaInstance != null, "kafka is null because the testCreateKafkaInstance has failed to create the Kafka instance");
    }

    void assertCredentials() {
        assumeTrue(Environment.BF2_GITHUB_TOKEN != null, "the BF2_GITHUB_TOKEN env is null");
        assumeTrue(Environment.SSO_USERNAME != null, "the SSO_USERNAME env is null");
        assumeTrue(Environment.SSO_PASSWORD != null, "the SSO_PASSWORD env is null");
    }

    void assertServiceAccount() {
        assumeTrue(serviceAccount != null, "serviceAccount is null because the testCreateServiceAccount has failed to create the Service Account");
    }

    void assertTopic() {
        assumeTrue(topic != null, "topic is null because the testCreateTopic has failed to create the topic on the Kafka instance");
    }

    @Test
    @Order(1)
    void testDownloadCLI() throws Throwable {
        assertCredentials();

        var downloader = CLIDownloader.defaultDownloader(vertx);

        // download the cli
        var binary = bwait(downloader.downloadCLIInTempDir());

        this.cli = new CLI(vertx, binary.directory, binary.name);

        LOGGER.info("validate cli");
        bwait(cli.help());
    }


    @Test
    @Order(2)
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void testLogin() throws Throwable {
        assertCLI();

        LOGGER.info("verify that we aren't logged-in");
        assertThrows(ProcessException.class, () -> bwait(cli.listKafka()));

        LOGGER.info("login the CLI");
        bwait(CLIUtils.login(vertx, cli, Environment.SSO_USERNAME, Environment.SSO_PASSWORD));

        LOGGER.info("verify that we are logged-in");
        bwait(cli.listKafka());

        loggedIn = true;
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    @Order(3)
    void testCreateServiceAccount() throws Throwable {
        assertLoggedIn();

        LOGGER.info("create a service account");
        serviceAccount = bwait(CLIUtils.createServiceAccount(vertx, cli, SERVICE_ACCOUNT_NAME));

        LOGGER.info("get the service account secret");
        ServiceAccountSecret secret = CLIUtils.getServiceAccountSecret(cli, serviceAccount.name);
        serviceAccount.clientSecret = secret.clientSecret;

        LOGGER.info("created service account {} with id {}", serviceAccount.name, serviceAccount.id);
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    @Order(4)
    void testCreateKafkaInstance() throws Throwable {
        assertLoggedIn();
        assertServiceAccount();

        LOGGER.info("create kafka instance with name {}", KAFKA_INSTANCE_NAME);
        var kafka = bwait(cli.createKafka(KAFKA_INSTANCE_NAME));

        LOGGER.info("wait for kafka instance: {}", kafka.id);
        kafkaInstance = bwait(waitForKafkaReady(vertx, cli, kafka.id));

        LOGGER.info("kafka instance {} with id {} is ready", kafka.name, kafka.id);
    }

    @Test
    @Order(5)
    void testGetStatusOfKafkaInstance() throws Throwable {
        assertLoggedIn();
        assertKafka();

        LOGGER.info("get kafka instance with name {}", KAFKA_INSTANCE_NAME);
        var kafka = bwait(cli.describeKafka(kafkaInstance.id));

        assertEquals("ready", kafka.status);
        LOGGER.info("found kafka instance {} with id {}", kafka.name, kafka.id);
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    @Order(6)
    void testCreateKafkaTopic() throws Throwable {
        assertLoggedIn();
        assertKafka();

        LOGGER.info("create kafka topic with name {}", KAFKA_INSTANCE_NAME);
        topic = bwait(cli.createTopic(TOPIC_NAME));

        assertEquals(topic.name, TOPIC_NAME);
        assertEquals(topic.partitions.size(), DEFAULT_PARTITIONS);
        LOGGER.info("topic created: {}", topic.name);
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    @Order(7)
    void testKafkaMessaging() throws Throwable {
        assertTopic();

        var bootstrapHost = kafkaInstance.bootstrapServerHost;
        var clientID = serviceAccount.clientID;
        var clientSecret = serviceAccount.clientSecret;

        bwait(testTopicWithOauth(
            vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            TOPIC_NAME,
            1000,
            10,
            100));
    }

    @Test
    @Order(8)
    void testUpdateKafkaTopic() throws Throwable {
        assertTopic();

        var retentionTime = "4";
        var retentionKey = "retention.ms";

        LOGGER.info("update kafka topic with name {}", TOPIC_NAME);
        var testTopic = bwait(cli.updateTopic(TOPIC_NAME, retentionTime));

        var retentionValue = testTopic.config.stream()
            .filter(conf -> conf.key.equals(retentionKey))
            .findFirst();

        assertTrue(retentionValue.isPresent(), "updated config not found");
        assertEquals(retentionValue.get().value, retentionTime);

        topic = testTopic;
    }

    @Test
    @Order(9)
    void testGetRetentionConfigFromTopic() throws Throwable {
        assertTopic();

        var retentionTime = "4";
        var retentionKey = "retention.ms";

        LOGGER.info("describe kafka topic with name {}", TOPIC_NAME);
        var testTopic = bwait(cli.describeTopic(TOPIC_NAME));

        assertEquals(testTopic.name, TOPIC_NAME);
        assertEquals(testTopic.partitions.size(), topic.partitions.size());

        var retentionValue = testTopic.config.stream()
            .filter(conf -> conf.key.equals(retentionKey))
            .findFirst();

        assertTrue(retentionValue.isPresent(), "updated config not found");
        assertEquals(retentionValue.get().value, retentionTime);
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    @Order(10)
    void testMessagingOnUpdatedTopic() throws Throwable {
        assertTopic();

        var bootstrapHost = kafkaInstance.bootstrapServerHost;
        var clientID = serviceAccount.clientID;
        var clientSecret = serviceAccount.clientSecret;

        bwait(testTopicWithOauth(
            vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            TOPIC_NAME,
            1000,
            10,
            100));
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    @Order(13)
    void testDeleteTopic() throws Throwable {
        assertTopic();

        LOGGER.info("delete topic: {}", TOPIC_NAME);
        bwait(cli.deleteTopic(TOPIC_NAME));

        LOGGER.info("wait for topic to be deleted: {}", TOPIC_NAME);
        bwait(waitForTopicDelete(vertx, cli, TOPIC_NAME)); // also verify that the topic doesn't exists anymore
    }

    @Test
    @Order(14)
    void testCreateAlreadyCreatedKafka() {
        assertLoggedIn();
        assertKafka();

        assertThrows(ProcessException.class, () -> bwait(cli.createKafka(KAFKA_INSTANCE_NAME)));
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    @Order(15)
    void testDeleteServiceAccount() throws Throwable {
        assertServiceAccount();

        bwait(cli.deleteServiceAccount(serviceAccount.id));
        LOGGER.info("service account {} with id {} deleted", serviceAccount.name, serviceAccount.id);

        // TODO: Verify that the service account doesn't exists
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    @Order(16)
    void testDeleteKafkaInstance() throws Throwable {
        assertLoggedIn();
        assertKafka();

        LOGGER.info("delete kafka instance {} with id {}", kafkaInstance.name, kafkaInstance.id);
        bwait(cli.deleteKafka(kafkaInstance.id));

        bwait(waitForKafkaDelete(vertx, cli, kafkaInstance.name));
    }
}
