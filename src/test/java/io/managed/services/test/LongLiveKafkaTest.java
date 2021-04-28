package io.managed.services.test;

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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.message;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopic;
import static io.managed.services.test.client.serviceapi.MetricsUtils.messageInTotalMetric;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.getKafkaByName;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.getServiceAccountByName;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsReady;
import static java.time.Duration.ofMinutes;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


@Tag(TestTag.SERVICE_API)
@Timeout(value = 5, unit = TimeUnit.MINUTES)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LongLiveKafkaTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(LongLiveKafkaTest.class);

    public static final String KAFKA_INSTANCE_NAME = "mk-e2e-ll-" + Environment.KAFKA_POSTFIX_NAME;
    public static final String SERVICE_ACCOUNT_NAME = "mk-e2e-ll-sa-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String[] TOPICS = {"ll-topic-az", "ll-topic-cb", "ll-topic-fc", "ll-topic-bf", "ll-topic-cd"};

    private final Vertx vertx = Vertx.vertx();

    private ServiceAPI api;
    private KafkaResponse kafka;
    private ServiceAccount serviceAccount;
    private boolean topic;

    @BeforeAll
    void bootstrap() throws Throwable {
        api = bwait(ServiceAPIUtils.serviceAPI(vertx));
    }

    void assertAPI() {
        assumeTrue(api != null, "api is null because the bootstrap has failed");
    }

    void assertKafka() {
        assumeTrue(kafka != null, "kafka is null because the testPresenceOfLongLiveKafkaInstance has failed to create the Kafka instance");
    }

    void assertServiceAccount() {
        assumeTrue(serviceAccount != null, "serviceAccount is null because the testPresenceOfTheServiceAccount has failed to create the Service Account");
    }

    void assertTopic() {
        assumeTrue(topic, "topic is null because the testPresenceOfTopic has failed to create the Topic");
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    @Order(1)
    void testPresenceOfLongLiveKafkaInstance() throws Throwable {
        assertAPI();

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

    @Test
    @Order(2)
    void testPresenceOfServiceAccount() throws Throwable {
        assertKafka();

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

    @Test
    @Order(3)
    void testCleanAdditionalServiceAccounts() throws Throwable {
        assertAPI();

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

    @Test
    @Order(3)
    void testPresenceOfTopics() throws Throwable {
        assertServiceAccount();

        String bootstrapHost = kafka.bootstrapServerHost;
        String clientID = serviceAccount.clientID;
        String clientSecret = serviceAccount.clientSecret;

        LOGGER.info("initialize kafka admin; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);

        var topics = Set.of(TOPICS);

        LOGGER.info("login to the kafka admin api: {}", bootstrapHost);
        var api = bwait(KafkaAdminAPIUtils.kafkaAdminAPI(vertx, bootstrapHost));

        LOGGER.info("apply topics: {}", topics);
        var missingTopics = bwait(KafkaAdminAPIUtils.applyTopics(api, topics));

        topic = true;

        assertTrue(missingTopics.isEmpty(), message("the topics: {} where missing and has been created", missingTopics));
    }


    @Test
    @Order(4)
    void testProduceAndConsumeKafkaMessages() throws Throwable {
        assertTopic();

        String bootstrapHost = kafka.bootstrapServerHost;
        String clientID = serviceAccount.clientID;
        String clientSecret = serviceAccount.clientSecret;

        for (var topic : TOPICS) {
            LOGGER.info("start testing topic: {}", topic);
            bwait(testTopic(vertx, bootstrapHost, clientID, clientSecret, topic, ofMinutes(1), 10, 7, 10, true));
        }
    }

    @Test
    @Order(5)
    void testMessageInTotalMetric() throws Throwable {
        assertAPI();

        LOGGER.info("start testing message in total metric");
        bwait(messageInTotalMetric(api, KAFKA_INSTANCE_NAME, SERVICE_ACCOUNT_NAME, vertx));
    }
}

