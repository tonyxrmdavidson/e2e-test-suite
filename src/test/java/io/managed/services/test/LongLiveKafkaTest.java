package io.managed.services.test;

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


@Test(groups = TestTag.SERVICE_API)
public class LongLiveKafkaTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(LongLiveKafkaTest.class);

    public static final String KAFKA_INSTANCE_NAME = "mk-e2e-ll-" + Environment.KAFKA_POSTFIX_NAME;
    public static final String SERVICE_ACCOUNT_NAME = "mk-e2e-ll-sa-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String[] TOPICS = {"ll-topic-az", "ll-topic-cb", "ll-topic-fc", "ll-topic-bf", "ll-topic-cd"};
    private static final String MULTI_PARTITION_TOPIC_NAME = "multi-partitions-topic";
    private static final String METRIC_TOPIC_NAME = "metric-test-topic";

    private final Vertx vertx = Vertx.vertx();

    private ServiceAPI api;
    private KafkaResponse kafka;
    private ServiceAccount serviceAccount;
    private boolean topic;
    private KafkaAdminAPI kafkaAdminAPI;

    @BeforeClass
    public void bootstrap() throws Throwable {
        api = bwait(ServiceAPIUtils.serviceAPI(vertx));
    }

    @AfterClass
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

    @Test(timeOut = 15 * MINUTES)
    public void testPresenceOfLongLiveKafkaInstance() throws Throwable {

        LOGGER.info("get kafka instance for name: {}", KAFKA_INSTANCE_NAME);
        var optionalKafka = bwait(getKafkaByName(api, KAFKA_INSTANCE_NAME));

        if (optionalKafka.isEmpty()) {
            LOGGER.error("kafka is not present: {}", KAFKA_INSTANCE_NAME);

            LOGGER.info("try to recreate the kafka instance: {}", KAFKA_INSTANCE_NAME);
            // Create Kafka Instance
            var kafkaPayload = new CreateKafkaPayload();
            kafkaPayload.name = KAFKA_INSTANCE_NAME;
            kafkaPayload.multiAZ = true;
            kafkaPayload.cloudProvider = "aws";
            kafkaPayload.region = "us-east-1";

            LOGGER.info("create kafka instance: {}", kafkaPayload.name);
            var k = bwait(api.createKafka(kafkaPayload, true));
            kafka = bwait(waitUntilKafkaIsReady(vertx, api, k.id));

            fail(message("for some reason the long living kafka instance with name: {} didn't exists anymore but we have recreate it", KAFKA_INSTANCE_NAME));
        }

        kafka = optionalKafka.get();
        LOGGER.info("kafka is present :{} and created at: {}", KAFKA_INSTANCE_NAME, kafka.createdAt);
    }

    @Test(priority = 1, timeOut = DEFAULT_TIMEOUT)
    public void testPresenceOfServiceAccount() throws Throwable {

        LOGGER.info("get service account by name: {}", SERVICE_ACCOUNT_NAME);
        var optionalSA = bwait(getServiceAccountByName(api, SERVICE_ACCOUNT_NAME));

        if (optionalSA.isEmpty()) {
            LOGGER.error("service account is not present: {}", SERVICE_ACCOUNT_NAME);

            LOGGER.info("try to recreate the service account: {}", SERVICE_ACCOUNT_NAME);
            // Create Service Account
            var serviceAccountPayload = new CreateServiceAccountPayload();
            serviceAccountPayload.name = SERVICE_ACCOUNT_NAME;

            LOGGER.info("create service account: {}", serviceAccountPayload.name);
            serviceAccount = bwait(api.createServiceAccount(serviceAccountPayload));
            fail(message("for some reason the long living service account with name: {} didn't exists anymore but we have recreate it", SERVICE_ACCOUNT_NAME));
        }

        LOGGER.info("reset credentials for service account: {}", SERVICE_ACCOUNT_NAME);
        serviceAccount = bwait(api.resetCredentialsServiceAccount(optionalSA.get().id));
    }

    @Test(priority = 2, timeOut = DEFAULT_TIMEOUT)
    public void testCleanAdditionalServiceAccounts() throws Throwable {

        var deleted = new ArrayList<String>();

        LOGGER.info("get all service accounts");
        var accountsList = bwait(api.getListOfServiceAccounts());

        var accounts = accountsList.items.stream()
            .filter(a -> a.owner.equals(Environment.SSO_USERNAME))
            .filter(a -> !a.name.equals(SERVICE_ACCOUNT_NAME))
            .collect(Collectors.toList());

        for (var a : accounts) {
            LOGGER.warn("delete service account: {}", a);
            bwait(api.deleteServiceAccount(a.id));
            deleted.add(a.name);
        }

        if (!deleted.isEmpty()) {
            fail(message("deleted {} service account that shouldn't exists: {}", deleted.size(), deleted));
        }
    }

    @Test(priority = 3, timeOut = DEFAULT_TIMEOUT)
    public void testPresenceOfTopics() throws Throwable {
        assertKafka();

        String bootstrapHost = kafka.bootstrapServerHost;

        var topics = new HashSet<>(Set.of(TOPICS));
        topics.add(METRIC_TOPIC_NAME);

        LOGGER.info("login to the kafka admin api: {}", bootstrapHost);
        var api = bwait(KafkaAdminAPIUtils.kafkaAdminAPI(vertx, bootstrapHost));
        kafkaAdminAPI = api;

        LOGGER.info("apply topics: {}", topics);
        var missingTopics = bwait(KafkaAdminAPIUtils.applyTopics(api, topics));

        topic = true;

        assertTrue(missingTopics.isEmpty(), message("the topics: {} where missing and has been created", missingTopics));
    }

    @Test(priority = 4, timeOut = 10 * MINUTES)
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

    @Test(priority = 4, timeOut = DEFAULT_TIMEOUT)
    void testTopicWithThreePartitionsAndThreeConsumers() throws Throwable {

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

    @Test(priority = 5, timeOut = DEFAULT_TIMEOUT)
    public void testMessageInTotalMetric() throws Throwable {
        assertKafka();
        assertServiceAccount();
        assertTopic();

        LOGGER.info("start testing message in total metric");
        bwait(messageInTotalMetric(vertx, api, kafka, serviceAccount, METRIC_TOPIC_NAME));
    }
}

