package io.managed.services.test.kafkainstances;


import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.kafka.KafkaAuthMethod;
import io.managed.services.test.client.kafka.KafkaConsumerClient;
import io.managed.services.test.client.kafkaadminapi.CreateTopicPayload;
import io.managed.services.test.client.kafkaadminapi.KafkaAdminAPI;
import io.managed.services.test.client.kafkaadminapi.KafkaAdminAPIUtils;
import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.client.serviceapi.CreateServiceAccountPayload;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.client.serviceapi.ServiceAccount;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.message;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopic;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopicWithMultipleConsumers;
import static io.managed.services.test.client.serviceapi.MetricsUtils.messageInTotalMetric;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.getKafkaByName;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.getServiceAccountByName;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsReady;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * Test a Long Live Kafka Instance that will be deleted only before it expires but otherwise it will continue to run
 * also after the tests are concluded. This is a very basic test to ensure that an update doesn't break
 * an existing Kafka Instance.
 */
@Test(groups = TestTag.SERVICE_API)
public class LongLiveKafkaInstanceTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(LongLiveKafkaInstanceTest.class);

    public static final String KAFKA_INSTANCE_NAME = "mk-e2e-ll-" + Environment.KAFKA_POSTFIX_NAME;
    public static final String SERVICE_ACCOUNT_NAME = "mk-e2e-ll-sa-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String[] TOPICS = {"ll-topic-az", "ll-topic-cb", "ll-topic-fc", "ll-topic-bf", "ll-topic-cd"};
    private static final String MULTI_PARTITION_TOPIC_NAME = "multi-partitions-topic";
    private static final String METRIC_TOPIC_NAME = "metric-test-topic";

    static final String TEST_CANARY_NAME = "__strimzi_canary";
    public static final String TEST_CANARY_GROUP = "canary-group";

    private final Vertx vertx = Vertx.vertx();

    private ServiceAPI serviceAPI;
    private KafkaResponse kafka;
    private ServiceAccount serviceAccount;
    private boolean topic;
    private KafkaAdminAPI kafkaAdminAPI;

    private static final int KAFKA_DELETION_TIMEOUT_IN_HOURS = 48;

    @BeforeClass
    public void bootstrap() throws Throwable {
        serviceAPI = bwait(ServiceAPIUtils.serviceAPI(vertx));
    }

    @AfterClass(timeOut = DEFAULT_TIMEOUT, alwaysRun = true)
    public void teardown() throws Throwable {
        try {
            bwait(kafkaAdminAPI.deleteTopic(MULTI_PARTITION_TOPIC_NAME));
        } catch (Throwable t) {
            LOGGER.error("failed to clean topic: {}", MULTI_PARTITION_TOPIC_NAME);
        }

        bwait(vertx.close());
    }

    private void assertKafka() {
        if (kafka == null)
            throw new SkipException("kafka is null because the testPresenceOfLongLiveKafkaInstance has failed to create the Kafka instance");
    }

    private void assertServiceAccount() {
        if (serviceAccount == null)
            throw new SkipException("serviceAccount is null because the testPresenceOfTheServiceAccount has failed to create the Service Account");
    }

    private void assertTopic() {
        if (!topic)
            throw new SkipException("topic is null because the testPresenceOfTopic has failed to create the Topic");
    }

    private List<String> applyTestTopics() throws Throwable {
        String bootstrapHost = kafka.bootstrapServerHost;
        var topics = new HashSet<>(Set.of(TOPICS));
        topics.add(METRIC_TOPIC_NAME);

        LOGGER.info("login to the kafka admin api: {}", bootstrapHost);
        kafkaAdminAPI = bwait(KafkaAdminAPIUtils.kafkaAdminAPI(vertx, bootstrapHost));

        LOGGER.info("apply topics: {}", topics);
        return bwait(KafkaAdminAPIUtils.applyTopics(kafkaAdminAPI, topics));
    }

    @Test(timeOut = 15 * MINUTES)
    public void recreateTheLongLiveKafkaInstanceIfItIsNearToExpiration() throws Throwable {
        // TODO: Use a Kafka Instance that never expires
        var optionalKafka = bwait(getKafkaByName(serviceAPI, KAFKA_INSTANCE_NAME));
        if (optionalKafka.isEmpty()) {
            fail(message("for some reason the long living kafka instance with name: {} doesn't exist, but it should", KAFKA_INSTANCE_NAME));
        }

        // kafka is present
        KafkaResponse kafkaResponse = optionalKafka.get();
        long upTimeInHours = kafkaResponse.getUpTimeInHours();
        LOGGER.info("long live kafka instance up time is: {} hours", upTimeInHours);

        if (upTimeInHours >= (KAFKA_DELETION_TIMEOUT_IN_HOURS - 1)) {
            LOGGER.info("kafka instance will be recreated due to it's up time: ({}) moving close to expiration: ({}) ", upTimeInHours, KAFKA_DELETION_TIMEOUT_IN_HOURS);
            // delete kafka instance which lives for almost max time.
            try {
                bwait(ServiceAPIUtils.deleteKafkaByNameIfExists(serviceAPI, KAFKA_INSTANCE_NAME));
            } catch (Throwable t) {
                LOGGER.error("failed to clean kafka instance: ", t);
            }

            LOGGER.info("waiting for deletion of old kafka instance");
            bwait(ServiceAPIUtils.waitUntilKafkaIsDeleted(vertx, serviceAPI, kafkaResponse.id));

            CreateKafkaPayload kafkaPayload = ServiceAPIUtils.createKafkaPayload(KAFKA_INSTANCE_NAME);

            LOGGER.info("waiting for creation of new kafka instance");
            kafkaResponse = bwait(serviceAPI.createKafka(kafkaPayload, true));
            kafka = bwait(waitUntilKafkaIsReady(vertx, serviceAPI, kafkaResponse.id));

            LOGGER.info("recreation of former kafka's instance topics");
            applyTestTopics();

        }
    }

    @Test(priority = 1, timeOut = 15 * MINUTES)
    public void testPresenceOfLongLiveKafkaInstance() throws Throwable {

        LOGGER.info("get kafka instance for name: {}", KAFKA_INSTANCE_NAME);
        var optionalKafka = bwait(getKafkaByName(serviceAPI, KAFKA_INSTANCE_NAME));

        if (optionalKafka.isEmpty()) {
            LOGGER.error("kafka is not present: {}", KAFKA_INSTANCE_NAME);

            LOGGER.info("try to recreate the kafka instance: {}", KAFKA_INSTANCE_NAME);
            // Create Kafka Instance
            CreateKafkaPayload kafkaPayload = ServiceAPIUtils.createKafkaPayload(KAFKA_INSTANCE_NAME);

            LOGGER.info("create kafka instance: {}", kafkaPayload.name);
            var k = bwait(serviceAPI.createKafka(kafkaPayload, true));
            kafka = bwait(waitUntilKafkaIsReady(vertx, serviceAPI, k.id));

            fail(message("for some reason the long living kafka instance with name: {} didn't exists anymore but we have recreated it", KAFKA_INSTANCE_NAME));
        }

        kafka = optionalKafka.get();
        LOGGER.info("kafka is present :{} and created at: {}", KAFKA_INSTANCE_NAME, kafka.createdAt);
    }

    @Test(priority = 2, timeOut = DEFAULT_TIMEOUT)
    public void testPresenceOfServiceAccount() throws Throwable {

        LOGGER.info("get service account by name: {}", SERVICE_ACCOUNT_NAME);
        var optionalSA = bwait(getServiceAccountByName(serviceAPI, SERVICE_ACCOUNT_NAME));

        if (optionalSA.isEmpty()) {
            LOGGER.error("service account is not present: {}", SERVICE_ACCOUNT_NAME);

            LOGGER.info("try to recreate the service account: {}", SERVICE_ACCOUNT_NAME);
            // Create Service Account
            var serviceAccountPayload = new CreateServiceAccountPayload();
            serviceAccountPayload.name = SERVICE_ACCOUNT_NAME;

            LOGGER.info("create service account: {}", serviceAccountPayload.name);

            serviceAccount = bwait(serviceAPI.createServiceAccount(serviceAccountPayload));
            fail(message("for some reason the long living service account with name: {} didn't exists anymore but we have recreate it", SERVICE_ACCOUNT_NAME));
        }

        LOGGER.info("reset credentials for service account: {}", SERVICE_ACCOUNT_NAME);
        serviceAccount = bwait(serviceAPI.resetCredentialsServiceAccount(optionalSA.get().id));
    }

    @Test(priority = 3, timeOut = DEFAULT_TIMEOUT)
    public void cleanAdditionalServiceAccounts() throws Throwable {

        var deleted = new ArrayList<String>();

        LOGGER.info("get all service accounts");
        var accountsList = bwait(serviceAPI.getListOfServiceAccounts());

        var accounts = accountsList.items.stream()
            .filter(a -> a.owner.equals(Environment.SSO_USERNAME))
            .filter(a -> !a.name.equals(SERVICE_ACCOUNT_NAME))
            .collect(Collectors.toList());

        for (var a : accounts) {
            LOGGER.warn("delete service account: {}", a);
            bwait(serviceAPI.deleteServiceAccount(a.id));
            deleted.add(a.name);
        }

        if (!deleted.isEmpty()) {
            fail(message("deleted {} service account that shouldn't exists: {}", deleted.size(), deleted));
        }
    }

    @Test(priority = 4, timeOut = DEFAULT_TIMEOUT)
    public void testPresenceOfTopics() throws Throwable {
        assertKafka();

        var missingTopics = applyTestTopics();
        topic = true;
        assertTrue(missingTopics.isEmpty(), message("the topics: {} where missing and has been created", missingTopics));
    }


    @Test(priority = 5, timeOut = DEFAULT_TIMEOUT)
    void testPresenceOfCanaryTopic() throws Throwable {
        assertKafka();

        var bootstrapHost = kafka.bootstrapServerHost;
        var clientID = serviceAccount.clientID;
        var clientSecret = serviceAccount.clientSecret;

        var admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);

        LOGGER.info("testing presence of canary topic: {}", TEST_CANARY_NAME);
        var response = bwait(admin.listTopics());
        boolean isCanaryTopicPresent = response.stream().filter(e -> e.equals(TEST_CANARY_NAME)).count() == 1;
        assertTrue(isCanaryTopicPresent, message("supposed canary topic {} is missing, whereas following topics are present {}", TEST_CANARY_NAME, response));
    }


    @Test(priority = 6, timeOut = DEFAULT_TIMEOUT, dependsOnMethods = {"testPresenceOfCanaryTopic"})
    void testCanaryLiveliness() throws Throwable {
        assertKafka();

        LOGGER.info("testing liveliness of canary: {}", TEST_CANARY_NAME);

        var bootstrapHost = kafka.bootstrapServerHost;
        var clientID = serviceAccount.clientID;
        var secret = serviceAccount.clientSecret;

        var consumerClient = new KafkaConsumerClient(
            vertx,
            bootstrapHost,
            clientID, secret,
            KafkaAuthMethod.OAUTH,
            TEST_CANARY_GROUP,
            "latest");
        bwait(consumerClient.receiveAsync(TEST_CANARY_NAME, 1));
    }


    @Test(priority = 7, timeOut = 10 * MINUTES)
    public void testProduceAndConsumeKafkaMessages() throws Throwable {
        assertKafka();
        assertServiceAccount();
        assertTopic();

        String bootstrapHost = kafka.bootstrapServerHost;
        String clientID = serviceAccount.clientID;
        String clientSecret = serviceAccount.clientSecret;

        for (var topic : TOPICS) {
            LOGGER.info("start testing topic: {}", topic);
            bwait(testTopic(vertx, bootstrapHost, clientID, clientSecret, topic, 10, 7, 10));
        }
    }

    @Test(priority = 8, timeOut = DEFAULT_TIMEOUT)
    void testTopicWithThreePartitionsAndThreeConsumers() throws Throwable {
        assertKafka();
        assertServiceAccount();
        assertTopic();

        var topicName = MULTI_PARTITION_TOPIC_NAME;

        LOGGER.info("create the topic with 3 partitions: {}", topicName);
        CreateTopicPayload topicPayload = KafkaAdminAPIUtils.setUpDefaultTopicPayload(topicName);
        topicPayload.settings.numPartitions = 3;
        bwait(kafkaAdminAPI.createTopic(topicPayload));

        var bootstrapHost = kafka.bootstrapServerHost;
        var clientID = serviceAccount.clientID;
        var clientSecret = serviceAccount.clientSecret;

        LOGGER.info("test the topic {} with 3 consumers", topicName);
        bwait(testTopicWithMultipleConsumers(
            vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            topicName,
            Duration.ofMinutes(1),
            1000,
            99,
            100,
            3));
    }

    @Test(priority = 9, timeOut = DEFAULT_TIMEOUT)
    public void testMessageInTotalMetric() throws Throwable {
        assertKafka();
        assertServiceAccount();
        assertTopic();

        LOGGER.info("start testing message in total metric");
        bwait(messageInTotalMetric(vertx, serviceAPI, kafka, serviceAccount, METRIC_TOPIC_NAME));
    }
}

