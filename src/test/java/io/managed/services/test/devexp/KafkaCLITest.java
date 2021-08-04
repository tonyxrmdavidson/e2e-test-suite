package io.managed.services.test.devexp;

import io.managed.services.test.Environment;
import io.managed.services.test.IsReady;
import io.managed.services.test.TestBase;
import io.managed.services.test.cli.CLI;
import io.managed.services.test.cli.CLIDownloader;
import io.managed.services.test.cli.CLIUtils;
import io.managed.services.test.cli.ProcessException;
import io.managed.services.test.client.kafka.KafkaAuthMethod;
import io.managed.services.test.client.kafka.KafkaConsumerClient;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.client.serviceapi.ServiceAccount;
import io.managed.services.test.client.serviceapi.ServiceAccountSecret;
import io.managed.services.test.client.serviceapi.TopicResponse;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.waitFor;
import static io.managed.services.test.cli.CLIUtils.waitForConsumerGroupDelete;
import static io.managed.services.test.cli.CLIUtils.waitForKafkaDelete;
import static io.managed.services.test.cli.CLIUtils.waitForKafkaReady;
import static io.managed.services.test.cli.CLIUtils.waitForTopicDelete;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopic;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/**
 * Test the application services CLI[1] kafka commands.
 * <p>
 * The tests download the CLI from github to the local machine where the test suite is running
 * and perform all operations using the CLI.
 * <p>
 * By default the latest version of the CLI is downloaded otherwise a specific version can be set using
 * the CLI_VERSION env. The CLI platform (linux, mac, win) and arch (amd64, arm) is automatically detected
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
    private KafkaResponse kafkaInstance;
    private ServiceAccount serviceAccount;
    private TopicResponse topic;
    private ServiceAPI serviceAPI;

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

    @AfterClass(timeOut = DEFAULT_TIMEOUT, alwaysRun = true)
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

        bwait(vertx.close());
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

        LOGGER.info("kafka instance is ready: {}", Json.encode(kafkaInstance));
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
    public void testKafkaInstanceTopic() throws Throwable {

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
    public void testKafkaInstanceUpdatedTopic() throws Throwable {

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
    public void testGetConsumerGroup() throws Throwable {

        // create consumer group
        serviceAPI = bwait(ServiceAPIUtils.serviceAPI(vertx));
        LOGGER.info("create or retrieve service account: {}", SERVICE_ACCOUNT_NAME);

        LOGGER.info("crete kafka consumer with group id: {}", CONSUMER_GROUP_NAME);
        var consumer = KafkaConsumerClient.createConsumer(vertx,
                kafkaInstance.bootstrapServerHost,
                serviceAccount.clientID,
                serviceAccount.clientSecret,
                KafkaAuthMethod.OAUTH,
                CONSUMER_GROUP_NAME,
                "latest");

        LOGGER.info("subscribe to topic: {}", TOPIC_NAME);
        consumer.subscribe(TOPIC_NAME);
        consumer.handler(r -> {
            // ignore
        });
        IsReady<Object> subscribed = last -> consumer.assignment().map(partitions -> {
            var o = partitions.stream().filter(p -> p.getTopic().equals(TOPIC_NAME)).findAny();
            return Pair.with(o.isPresent(), null);
        });
        bwait(waitFor(vertx, "consumer group to subscribe", ofSeconds(2), ofMinutes(2), subscribed));
        bwait(consumer.close());

        // list all consumer groups
        LOGGER.info("get list of consumer group and check if expected group (with name {})  is present", TOPIC_NAME);
        var consumerGroup = bwait(CLIUtils.getConsumerGroupByName(cli, CONSUMER_GROUP_NAME));
        assertTrue(consumerGroup.isPresent(), "Consumer group isn't present amongst listed groups");

        // additional check for description of the same consumer Group
        var consumerGroupDescription = bwait(cli.describeConsumerGroup(CONSUMER_GROUP_NAME));
        assertNotNull(consumerGroupDescription, "consumer group isn't present");
    }

    @Test(dependsOnMethods = "testGetConsumerGroup", priority = 1, timeOut = DEFAULT_TIMEOUT)
    public void testDeleteConsumerGroup() throws Throwable {

        LOGGER.info("delete consumer group with name (id): {}",  CONSUMER_GROUP_NAME);
        bwait(cli.deleteConsumerGroup(CONSUMER_GROUP_NAME));
        bwait(waitForConsumerGroupDelete(vertx, cli, CONSUMER_GROUP_NAME));
    }

    @Test(dependsOnMethods = "testCreateKafkaTopic", priority = 2, timeOut = DEFAULT_TIMEOUT)
    public void testDeleteTopic() throws Throwable {

        LOGGER.info("delete topic: {}", TOPIC_NAME);
        bwait(cli.deleteTopic(TOPIC_NAME));

        LOGGER.info("wait for topic to be deleted: {}", TOPIC_NAME);
        bwait(waitForTopicDelete(vertx, cli, TOPIC_NAME)); // also verify that the topic doesn't exists anymore
    }

    @Test(dependsOnMethods = "testCreateServiceAccount", priority = 2, timeOut = DEFAULT_TIMEOUT)
    public void testDeleteServiceAccount() throws Throwable {

        bwait(cli.deleteServiceAccount(serviceAccount.id));
        LOGGER.info("service account {} with id {} deleted", serviceAccount.name, serviceAccount.id);

        // TODO: Verify that the service account doesn't exists
    }

    @Test(dependsOnMethods = "testCreateKafkaInstance", priority = 3, timeOut = DEFAULT_TIMEOUT)
    public void testDeleteKafkaInstance() throws Throwable {

        LOGGER.info("delete kafka instance {} with id {}", kafkaInstance.name, kafkaInstance.id);
        bwait(cli.deleteKafka(kafkaInstance.id));

        bwait(waitForKafkaDelete(vertx, cli, kafkaInstance.name));
    }

    @Test(dependsOnMethods = "testLogin", priority = 3, timeOut = DEFAULT_TIMEOUT)
    public void testLogout() throws Throwable {

        LOGGER.info("verify that we are logged-in");
        bwait(cli.listKafka()); // successfully run cli command while logged in

        LOGGER.info("logout");
        bwait(cli.logout());

        LOGGER.info("verify that we are logged-in");
        assertThrows(ProcessException.class, () -> bwait(cli.listKafka())); // unable to run the same command after logout
    }
}
