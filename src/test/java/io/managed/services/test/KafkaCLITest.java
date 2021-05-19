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
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.cli.CLIUtils.waitForKafkaDelete;
import static io.managed.services.test.cli.CLIUtils.waitForKafkaReady;
import static io.managed.services.test.cli.CLIUtils.waitForTopicDelete;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


@Test(groups = TestTag.CLI)
public class KafkaCLITest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(KafkaCLITest.class);

    private static final String KAFKA_INSTANCE_NAME = "cli-e2e-test-instance-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String SERVICE_ACCOUNT_NAME = "cli-e2e-service-account-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String TOPIC_NAME = "cli-e2e-test-topic";
    private static final int DEFAULT_PARTITIONS = 1;

    private final Vertx vertx = Vertx.vertx();

    private CLI cli;
    private KafkaResponse kafkaInstance;
    private ServiceAccount serviceAccount;
    private TopicResponse topic;

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

    @AfterClass(timeOut = DEFAULT_TIMEOUT)
    public void clean() throws Throwable {

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

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testDownloadCLI() throws Throwable {

        var downloader = CLIDownloader.defaultDownloader(vertx);

        // download the cli
        var binary = bwait(downloader.downloadCLIInTempDir());

        this.cli = new CLI(vertx, binary.directory, binary.name);

        LOGGER.info("validate cli");
        bwait(cli.help());
    }


    @Test(dependsOnMethods = "testDownloadCLI", timeOut = DEFAULT_TIMEOUT)
    public void testLogin() throws Throwable {

        LOGGER.info("verify that we aren't logged-in");
        assertThrows(ProcessException.class, () -> bwait(cli.listKafka()));

        LOGGER.info("login the CLI");
        bwait(CLIUtils.login(vertx, cli, Environment.SSO_USERNAME, Environment.SSO_PASSWORD));

        LOGGER.info("verify that we are logged-in");
        bwait(cli.listKafka());
    }

    @Test(dependsOnMethods = "testLogin", timeOut = DEFAULT_TIMEOUT)
    public void testCreateServiceAccount() throws Throwable {

        LOGGER.info("create a service account");
        serviceAccount = bwait(CLIUtils.createServiceAccount(vertx, cli, SERVICE_ACCOUNT_NAME));

        LOGGER.info("get the service account secret");
        ServiceAccountSecret secret = CLIUtils.getServiceAccountSecret(cli, serviceAccount.name);
        serviceAccount.clientSecret = secret.clientSecret;

        LOGGER.info("created service account {} with id {}", serviceAccount.name, serviceAccount.id);
    }

    @Test(dependsOnMethods = "testLogin", timeOut = 15 * MINUTES)
    public void testCreateKafkaInstance() throws Throwable {

        LOGGER.info("create kafka instance with name {}", KAFKA_INSTANCE_NAME);
        var kafka = bwait(cli.createKafka(KAFKA_INSTANCE_NAME));

        LOGGER.info("wait for kafka instance: {}", kafka.id);
        kafkaInstance = bwait(waitForKafkaReady(vertx, cli, kafka.id));

        LOGGER.info("kafka instance {} with id {} is ready", kafka.name, kafka.id);
    }

    @Test(dependsOnMethods = "testCreateKafkaInstance", timeOut = DEFAULT_TIMEOUT)
    public void testGetStatusOfKafkaInstance() throws Throwable {

        LOGGER.info("get kafka instance with name {}", KAFKA_INSTANCE_NAME);
        var kafka = bwait(cli.describeKafka(kafkaInstance.id));

        assertEquals("ready", kafka.status);
        LOGGER.info("found kafka instance {} with id {}", kafka.name, kafka.id);
    }

    @Test(dependsOnMethods = "testCreateKafkaInstance", timeOut = DEFAULT_TIMEOUT)
    public void testCreateKafkaTopic() throws Throwable {

        LOGGER.info("create kafka topic with name {}", KAFKA_INSTANCE_NAME);
        topic = bwait(cli.createTopic(TOPIC_NAME));

        assertEquals(topic.name, TOPIC_NAME);
        assertEquals(topic.partitions.size(), DEFAULT_PARTITIONS);
        LOGGER.info("topic created: {}", topic.name);
    }

    @Test(dependsOnMethods = "testCreateKafkaTopic", timeOut = DEFAULT_TIMEOUT)
    public void testKafkaMessaging() throws Throwable {

        var bootstrapHost = kafkaInstance.bootstrapServerHost;
        var clientID = serviceAccount.clientID;
        var clientSecret = serviceAccount.clientSecret;

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

    @Test(dependsOnMethods = "testCreateKafkaTopic", timeOut = DEFAULT_TIMEOUT)
    public void testUpdateKafkaTopic() throws Throwable {

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

    @Test(dependsOnMethods = "testUpdateKafkaTopic", timeOut = DEFAULT_TIMEOUT)
    public void testGetRetentionConfigFromTopic() throws Throwable {

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

    @Test(dependsOnMethods = "testUpdateKafkaTopic", timeOut = DEFAULT_TIMEOUT)
    public void testMessagingOnUpdatedTopic() throws Throwable {

        var bootstrapHost = kafkaInstance.bootstrapServerHost;
        var clientID = serviceAccount.clientID;
        var clientSecret = serviceAccount.clientSecret;

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

    @Test(dependsOnMethods = "testCreateKafkaTopic", priority = 1, timeOut = DEFAULT_TIMEOUT)
    public void testDeleteTopic() throws Throwable {

        LOGGER.info("delete topic: {}", TOPIC_NAME);
        bwait(cli.deleteTopic(TOPIC_NAME));

        LOGGER.info("wait for topic to be deleted: {}", TOPIC_NAME);
        bwait(waitForTopicDelete(vertx, cli, TOPIC_NAME)); // also verify that the topic doesn't exists anymore
    }

    @Test(dependsOnMethods = "testCreateKafkaInstance", timeOut = DEFAULT_TIMEOUT)
    public void testCreateAlreadyCreatedKafka() {
        assertThrows(ProcessException.class, () -> bwait(cli.createKafka(KAFKA_INSTANCE_NAME)));
    }

    @Test(dependsOnMethods = "testCreateServiceAccount", priority = 1, timeOut = DEFAULT_TIMEOUT)
    public void testDeleteServiceAccount() throws Throwable {

        bwait(cli.deleteServiceAccount(serviceAccount.id));
        LOGGER.info("service account {} with id {} deleted", serviceAccount.name, serviceAccount.id);

        // TODO: Verify that the service account doesn't exists
    }

    @Test(dependsOnMethods = "testCreateKafkaInstance", priority = 2, timeOut = DEFAULT_TIMEOUT)
    public void testDeleteKafkaInstance() throws Throwable {

        LOGGER.info("delete kafka instance {} with id {}", kafkaInstance.name, kafkaInstance.id);
        bwait(cli.deleteKafka(kafkaInstance.id));

        bwait(waitForKafkaDelete(vertx, cli, kafkaInstance.name));
    }
}
