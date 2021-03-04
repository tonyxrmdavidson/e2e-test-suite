package io.managed.services.test;

import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.client.serviceapi.CreateServiceAccountPayload;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.client.serviceapi.ServiceAccount;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.managed.services.test.TestUtils.await;
import static io.managed.services.test.TestUtils.forEach;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopic;
import static io.managed.services.test.client.kafka.KafkaUtils.applyTopics;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.getKafkaByName;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.getServiceAccountByName;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsReady;
import static java.text.MessageFormat.format;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


@Tag(TestTag.SERVICE_API)
@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ServiceAPILongLiveTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPITest.class);

    private static final String KAFKA_INSTANCE_NAME = "mk-e2e-ll-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String SERVICE_ACCOUNT_NAME = "mk-e2e-ll-sa-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String[] TOPICS = {"ll-topic-az", "ll-topic-cb", "ll-topic-fc", "ll-topic-bf", "ll-topic-cd"};

    ServiceAPI api;

    KafkaResponse kafka;
    ServiceAccount serviceAccount;
    boolean topic;

    @BeforeAll
    void bootstrap(Vertx vertx) {
        api = await(ServiceAPIUtils.serviceAPI(vertx));
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
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    @Order(1)
    void testPresenceOfLongLiveKafkaInstance(Vertx vertx) {
        assertAPI();

        LOGGER.info("get kafka instance for name: {}", KAFKA_INSTANCE_NAME);
        var optionalKafka = await(getKafkaByName(api, KAFKA_INSTANCE_NAME));
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
            var k = await(api.createKafka(kafkaPayload, true));
            kafka = await(waitUntilKafkaIsReady(vertx, api, k.id));

            fail(String.format("for some reason the long living kafka instance with name: %s didn't exists anymore but we have recreate it", KAFKA_INSTANCE_NAME));
        }

        kafka = optionalKafka.get();
        LOGGER.info("kafka is present :{} and created at: {}", KAFKA_INSTANCE_NAME, kafka.createdAt);
    }

    @Test
    @Order(2)
    void testPresenceOfServiceAccount() {
        assertKafka();

        LOGGER.info("get service account by name: {}", SERVICE_ACCOUNT_NAME);
        var optionalAccount = await(getServiceAccountByName(api, SERVICE_ACCOUNT_NAME));
        if (optionalAccount.isEmpty()) {
            LOGGER.error("service account is not present: {}", SERVICE_ACCOUNT_NAME);

            LOGGER.info("try to recreate the service account: {}", SERVICE_ACCOUNT_NAME);
            // Create Service Account
            var serviceAccountPayload = new CreateServiceAccountPayload();
            serviceAccountPayload.name = SERVICE_ACCOUNT_NAME;

            LOGGER.info("create service account: {}", serviceAccountPayload.name);
            serviceAccount = await(api.createServiceAccount(serviceAccountPayload));

            fail(String.format("for some reason the long living service account with name: %s didn't exists anymore but we have recreate it", SERVICE_ACCOUNT_NAME));
        }

        LOGGER.info("reset credentials for service account: {}", SERVICE_ACCOUNT_NAME);
        serviceAccount = await(api.resetCredentialsServiceAccount(optionalAccount.get().id));
    }

    @Test
    @Order(3)
    void testPresenceOfTopics() {
        assertServiceAccount();

        String bootstrapHost = kafka.bootstrapServerHost;
        String clientID = serviceAccount.clientID;
        String clientSecret = serviceAccount.clientSecret;

        LOGGER.info("initialize kafka admin; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        var admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);

        var topics = Set.of(TOPICS);
        LOGGER.info("apply topics: {}", topics);
        var missingTopics = await(applyTopics(admin, topics));

        topic = true;

        // log failure if we had to recreate some topics
        assertTrue(missingTopics.isEmpty(), format("the topics: {1} where missing and has been created", missingTopics));
    }

    @Test
    @Order(4)
    void testProduceAndConsumeKafkaMessages(Vertx vertx) {
        assertTopic();

        String bootstrapHost = kafka.bootstrapServerHost;
        String clientID = serviceAccount.clientID;
        String clientSecret = serviceAccount.clientSecret;


        await(forEach(Set.of(TOPICS), topic -> {
            LOGGER.info("start testing topic: {}", topic);
            return testTopic(vertx, bootstrapHost, clientID, clientSecret, topic, 10, 7, 10);
        }));
    }
}

