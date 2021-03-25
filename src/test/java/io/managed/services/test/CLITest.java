package io.managed.services.test;

import io.managed.services.test.cli.CLI;
import io.managed.services.test.cli.CLIDownloader;
import io.managed.services.test.cli.CLIUtils;
import io.managed.services.test.cli.ProcessException;
import io.managed.services.test.client.kafka.KafkaProducerClient;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.client.serviceapi.ServiceAccount;
import io.managed.services.test.client.serviceapi.ServiceAccountSecret;
import io.managed.services.test.client.serviceapi.TopicConfig;
import io.managed.services.test.client.serviceapi.TopicResponse;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.managed.services.test.cli.CLIUtils.waitForKafkaDelete;
import static io.managed.services.test.cli.CLIUtils.waitForKafkaReady;
import static io.managed.services.test.cli.CLIUtils.waitForTopicDelete;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopic;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


@Tag(TestTag.CLI)
@ExtendWith(VertxExtension.class)
@Timeout(value = 5, timeUnit = TimeUnit.MINUTES)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CLITest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(CLITest.class);

    static final String DOWNLOAD_ORG = "bf2fc6cc711aee1a0c2a";
    static final String DOWNLOAD_REPO = "cli";
    static final String KAFKA_INSTANCE_NAME = "cli-e2e-test-instance-" + Environment.KAFKA_POSTFIX_NAME;
    static final String SERVICE_ACCOUNT_NAME = "cli-e2e-service-account-" + Environment.KAFKA_POSTFIX_NAME;
    static final String TOPIC_NAME = "cli-e2e-test-topic";
    static final int DEFAULT_PARTITIONS = 1;

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

    @AfterAll
    void clean(Vertx vertx, VertxTestContext context) {
        ServiceAPIUtils.serviceAPI(vertx)

                // clean service account
                .compose(api -> cleanServiceAccount(api)

                        // clean kafka instance
                        .compose(__ -> cleanKafkaInstance(api)))

                .compose(__ -> {
                    if (cli != null) {
                        LOGGER.info("delete workdir: {}", cli.getWorkdir());
                        return vertx.fileSystem().deleteRecursive(cli.getWorkdir(), true);
                    }
                    return Future.succeededFuture();
                })

                .onComplete(context.succeedingThenComplete());
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
    void testDownloadCLI(Vertx vertx, VertxTestContext context) {
        assertCredentials();

        var downloader = new CLIDownloader(
                vertx,
                Environment.BF2_GITHUB_TOKEN,
                DOWNLOAD_ORG,
                DOWNLOAD_REPO,
                Environment.CLI_VERSION,
                Environment.CLI_PLATFORM,
                Environment.CLI_ARCH);

        // download the cli
        downloader.downloadCLIInTempDir()

                .compose(binary -> {
                    this.cli = new CLI(binary.directory, binary.name);

                    LOGGER.info("validate cli");
                    return cli.help();
                })

                .onComplete(context.succeedingThenComplete());
    }


    @Test
    @Order(2)
    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    void testLogin(Vertx vertx, VertxTestContext context) {
        assertCLI();

        LOGGER.info("verify that we aren't logged-in");
        cli.listKafka(vertx)
                .compose(r -> Future.failedFuture("cli kafka list should fail because we haven't log-in yet"))
                .recover(t -> {
                    if (t instanceof ProcessException) {
                        if (((ProcessException) t).process.exitValue() == 1) {
                            LOGGER.info("we haven't log-in yet");
                            return Future.succeededFuture();
                        }
                    }
                    return Future.failedFuture(t);
                })
                .compose(__ -> {
                    LOGGER.info("login the CLI");
                    return CLIUtils.login(vertx, cli, Environment.SSO_USERNAME, Environment.SSO_PASSWORD);
                })

                .compose(__ -> {

                    LOGGER.info("verify that we are logged-in");
                    return cli.listKafka(vertx);
                })

                .onSuccess(__ -> loggedIn = true)

                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    @Order(3)
    void testCreateServiceAccount(Vertx vertx, VertxTestContext context) {
        assertLoggedIn();

        CLIUtils.createServiceAccount(vertx, cli, SERVICE_ACCOUNT_NAME)
                .onSuccess(sa -> {
                    serviceAccount = sa;
                    try {
                        ServiceAccountSecret secret = CLIUtils.getServiceAccountSecret(cli, sa.name);
                        sa.clientSecret = secret.password;
                    } catch (IOException e) {
                        context.failNow(e);
                    }
                    LOGGER.info("Created serviceaccount {} with id {} and secret {}",
                            serviceAccount.name,
                            serviceAccount.id,
                            serviceAccount.clientSecret);
                })
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    @Order(4)
    void testCreateKafkaInstance(Vertx vertx, VertxTestContext context) {
        assertLoggedIn();
        assertServiceAccount();

        LOGGER.info("create kafka instance with name {}", KAFKA_INSTANCE_NAME);
        cli.createKafka(vertx, KAFKA_INSTANCE_NAME)
                .compose(kafka -> {
                    LOGGER.info("created kafka instance {} with id {}", kafka.name, kafka.id);
                    return waitForKafkaReady(vertx, cli, kafka.id)
                            .onSuccess(k -> {
                                LOGGER.info("kafka instance {} with id {} is ready", kafka.name, kafka.id);
                                kafkaInstance = k;
                            });
                })
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(5)
    void testGetStatusOfKafkaInstance(Vertx vertx, VertxTestContext context) {
        assertLoggedIn();
        assertKafka();

        LOGGER.info("Get kafka instance with name {}", KAFKA_INSTANCE_NAME);
        cli.describeKafka(vertx, kafkaInstance.id)
                .onSuccess(kafka -> context.verify(() -> {
                    assertEquals("ready", kafka.status);
                    LOGGER.info("found kafka instance {} with id {}", kafka.name, kafka.id);
                }))

                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    @Order(6)
    void testCreateKafkaTopic(Vertx vertx, VertxTestContext context) {
        assertLoggedIn();
        assertKafka();
        LOGGER.info("Create kafka topic with name {}", KAFKA_INSTANCE_NAME);
        cli.createTopic(vertx, TOPIC_NAME)
                .onSuccess(testTopic -> context.verify(() -> {
                    assertEquals(testTopic.name, TOPIC_NAME);
                    assertEquals(testTopic.partitions.size(), DEFAULT_PARTITIONS);
                    topic = testTopic;
                }))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    @Order(7)
    void testKafkaMessaging(Vertx vertx, VertxTestContext context) {
        assertTopic();

        var bootstrapHost = kafkaInstance.bootstrapServerHost;
        var clientID = serviceAccount.clientID;
        var clientSecret = serviceAccount.clientSecret;

        testTopic(vertx, bootstrapHost, clientID, clientSecret, TOPIC_NAME, 1000, 10, 100, true)
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(8)
    void testUpdateKafkaTopic(Vertx vertx, VertxTestContext context) {
        assertTopic();
        String retentionTime = "4";
        String retentionKey = "retention.ms";
        LOGGER.info("Update kafka topic with name {}", TOPIC_NAME);
        cli.updateTopic(vertx, TOPIC_NAME, retentionTime)
                .onSuccess(testTopic -> context.verify(() -> {
                    Optional<TopicConfig> retentionValue = testTopic.config.stream().filter(conf -> conf.key.equals(retentionKey))
                            .findFirst();
                    if (retentionValue.isPresent()) {
                        assertEquals(retentionValue.get().value, retentionTime);
                    } else {
                        context.failNow("Updated config not found");
                    }
                    topic = testTopic;
                }))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(9)
    void testGetRetentionConfigFromTopic(Vertx vertx, VertxTestContext context) {
        assertTopic();
        String retentionTime = "4";
        String retentionKey = "retention.ms";
        LOGGER.info("Describe kafka topic with name {}", TOPIC_NAME);
        cli.describeTopic(vertx, TOPIC_NAME)
                .onSuccess(testTopic -> context.verify(() -> {
                    assertEquals(testTopic.name, TOPIC_NAME);
                    assertEquals(testTopic.partitions.size(), topic.partitions.size());
                    Optional<TopicConfig> retentionValue = testTopic.config.stream().filter(conf -> conf.key.equals(retentionKey))
                            .findFirst();
                    if (retentionValue.isPresent()) {
                        assertEquals(retentionValue.get().value, retentionTime);
                    } else {
                        context.failNow("Updated config not found");
                    }
                }))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    @Order(10)
    void testMessagingOnUpdatedTopic(Vertx vertx, VertxTestContext context) {
        assertTopic();

        var bootstrapHost = kafkaInstance.bootstrapServerHost;
        var clientID = serviceAccount.clientID;
        var clientSecret = serviceAccount.clientSecret;

        testTopic(vertx, bootstrapHost, clientID, clientSecret, TOPIC_NAME, 1000, 10, 100, true)
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    @Order(11)
    void testPlainMessaging(Vertx vertx, VertxTestContext context) {
        assertTopic();

        var bootstrapHost = kafkaInstance.bootstrapServerHost;
        var clientID = serviceAccount.clientID;
        var clientSecret = serviceAccount.clientSecret;

        testTopic(vertx, bootstrapHost, clientID, clientSecret, TOPIC_NAME, 1000, 10, 100, false)
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    @Order(12)
    void testFailedPlainMessaging(Vertx vertx, VertxTestContext context) {
        assertTopic();
        var bootstrapHost = kafkaInstance.bootstrapServerHost;
        var clientID = serviceAccount.clientID;

        testTopic(vertx, bootstrapHost, clientID, "invalid", TOPIC_NAME, 1000, 10, 100, false)
                .onComplete(context.failingThenComplete());
    }

    @Test
    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    @Order(13)
    void testFailedOauthMessaging(Vertx vertx, VertxTestContext context) {
        assertTopic();
        var bootstrapHost = kafkaInstance.bootstrapServerHost;
        var clientID = serviceAccount.clientID;

        try {
            KafkaProducerClient producer = new KafkaProducerClient(vertx, bootstrapHost, clientID, "invalid", true);
            producer.close();
            context.failNow("Producer successfully connected");
        } catch (Exception e) {
            context.completeNow();
        }
    }

    @Test
    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    @Order(14)
    void testDeleteTopic(Vertx vertx, VertxTestContext context) {
        assertTopic();
        cli.deleteTopic(vertx, TOPIC_NAME)
                .compose(__ -> waitForTopicDelete(vertx, cli, TOPIC_NAME))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(15)
    void testCreateAlreadyCreatedKafka(Vertx vertx, VertxTestContext context) {
        assertLoggedIn();
        assertKafka();

        cli.createKafka(vertx, KAFKA_INSTANCE_NAME)
                .compose(r -> Future.failedFuture("Create kafka with same name should fail"))
                .recover(throwable -> {
                    if (throwable instanceof Exception) {
                        LOGGER.info("Create kafka with already exists name failed");
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })

                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    @Order(16)
    void testDeleteServiceAccount(Vertx vertx, VertxTestContext context) {
        assertServiceAccount();
        cli.deleteServiceAccount(vertx, serviceAccount.id)
                .onSuccess(__ ->
                        LOGGER.info("Serviceaccount {} with id {} deleted", serviceAccount.name, serviceAccount.id))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    @Order(17)
    void testDeleteKafkaInstance(Vertx vertx, VertxTestContext context) {
        assertLoggedIn();
        assertKafka();

        LOGGER.info("Delete kafka instance {} with id {}", kafkaInstance.name, kafkaInstance.id);
        cli.deleteKafka(vertx, kafkaInstance.id)
                .compose(__ -> waitForKafkaDelete(vertx, cli, kafkaInstance.name))
                .onComplete(context.succeedingThenComplete());
    }
}
