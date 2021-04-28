package io.managed.services.test;

import io.managed.services.test.client.exception.HTTPConflictException;
import io.managed.services.test.client.kafka.KafkaProducerClient;
import io.managed.services.test.client.kafkaadminapi.KafkaAdminAPI;
import io.managed.services.test.client.kafkaadminapi.KafkaAdminAPIUtils;
import io.managed.services.test.client.kafkaadminapi.Topic;
import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.client.serviceapi.CreateServiceAccountPayload;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.client.serviceapi.ServiceAccount;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.sleep;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopicPlain;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopicWithOauth;
import static io.managed.services.test.client.serviceapi.MetricsUtils.messageInTotalMetric;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteKafkaByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteServiceAccountByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.getKafkaByName;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsDeleted;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsReady;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


@Tag(TestTag.SERVICE_API)
@Timeout(value = 5, unit = TimeUnit.MINUTES)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ServiceAPITest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPITest.class);

    static final String KAFKA_INSTANCE_NAME = "mk-e2e-" + Environment.KAFKA_POSTFIX_NAME;
    static final String KAFKA2_INSTANCE_NAME = "mk-e2e-2-" + Environment.KAFKA_POSTFIX_NAME;
    static final String SERVICE_ACCOUNT_NAME = "mk-e2e-sa-" + Environment.KAFKA_POSTFIX_NAME;
    static final String TOPIC_NAME = "test-topic";

    private final Vertx vertx = Vertx.vertx();

    private ServiceAPI api;
    private KafkaResponse kafka;
    private ServiceAccount serviceAccount;
    private Topic topic;

    KafkaAdminAPI adminApi;

    @BeforeAll
    void bootstrap() throws Throwable {
        api = bwait(ServiceAPIUtils.serviceAPI(vertx));
    }

    private Future<Void> deleteKafkaInstance(String instanceName) {
        return deleteKafkaByNameIfExists(api, instanceName);
    }

    private Future<Void> deleteServiceAccount() {
        return deleteServiceAccountByNameIfExists(api, SERVICE_ACCOUNT_NAME);
    }

    @AfterAll
    void teardown() {

        // delete kafka instance
        try {
            bwait(deleteKafkaInstance(KAFKA_INSTANCE_NAME));
        } catch (Throwable t) {
            LOGGER.error("clean main kafka instance error: ", t);
        }

        try {
            bwait(deleteKafkaInstance(KAFKA2_INSTANCE_NAME));
        } catch (Throwable t) {
            LOGGER.error("clean second kafka instance error: ", t);
        }

        // delete service account
        try {
            bwait(deleteServiceAccount());
        } catch (Throwable t) {
            LOGGER.error("clean service account error: ", t);
        }
    }

    void assertAPI() {
        assumeTrue(api != null, "api is null because the bootstrap has failed");
    }

    void assertKafka() {
        assumeTrue(kafka != null, "kafka is null because the testCreateKafkaInstance has failed to create the Kafka instance");
    }

    void assertServiceAccount() {
        assumeTrue(serviceAccount != null, "serviceAccount is null because the testCreateServiceAccount has failed to create the Service Account");
    }

    void assertTopic() {
        assumeTrue(topic != null, "topic is null because the testCreateTopic has failed to create the topic on the Kafka instance");
    }

    /**
     * Create a new Kafka instance
     */
    @Test
    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    @Order(1)
    void testCreateKafkaInstance() throws Throwable {
        assertAPI();

        // Create Kafka Instance
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        // add postfix to the name based on owner
        kafkaPayload.name = KAFKA_INSTANCE_NAME;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";

        LOGGER.info("create kafka instance: {}", kafkaPayload.name);
        var k = bwait(api.createKafka(kafkaPayload, true));
        kafka = bwait(waitUntilKafkaIsReady(vertx, api, k.id));
    }

    @Test
    @Order(1)
    void testCreateServiceAccount() throws Throwable {
        assertAPI();

        // Create Service Account
        CreateServiceAccountPayload serviceAccountPayload = new CreateServiceAccountPayload();
        serviceAccountPayload.name = SERVICE_ACCOUNT_NAME;

        LOGGER.info("create service account: {}", serviceAccountPayload.name);
        serviceAccount = bwait(api.createServiceAccount(serviceAccountPayload));
    }

    @Test
    @Order(2)
    void testCreateTopic() throws Throwable {
        assertKafka();

        var bootstrapHost = kafka.bootstrapServerHost;

        LOGGER.info("create topic with name {} on the instance: {}", TOPIC_NAME, bootstrapHost);
        var admin = bwait(KafkaAdminAPIUtils.kafkaAdminAPI(vertx, bootstrapHost));

        topic = bwait(KafkaAdminAPIUtils.createDefaultTopic(admin, TOPIC_NAME));
    }

    @Test
    @Order(4)
    void testMessageInTotalMetric() throws Throwable {
        assertAPI();

        LOGGER.info("start testing message in total metric");
        bwait(messageInTotalMetric(vertx, api, KAFKA_INSTANCE_NAME, serviceAccount));
    }

    @Test
    @Order(3)
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void testOAuthMessaging() throws Throwable {
        assertKafka();
        assertServiceAccount();
        assertTopic();

        var bootstrapHost = kafka.bootstrapServerHost;
        var clientID = serviceAccount.clientID;
        var clientSecret = serviceAccount.clientSecret;
        var topicName = topic.name;

        bwait(testTopicWithOauth(
            vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            topicName,
            1000,
            10,
            100));
    }


    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    @Order(3)
    void testFailedOauthMessaging() {
        assertKafka();
        assertServiceAccount();
        assertTopic();

        var bootstrapHost = kafka.bootstrapServerHost;
        var clientID = serviceAccount.clientID;
        var topicName = topic.name;

        assertThrows(KafkaException.class, () -> bwait(testTopicWithOauth(
            vertx,
            bootstrapHost,
            clientID,
            "invalid",
            topicName,
            1,
            10,
            11)));
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    @Order(3)
    void testPlainMessaging() throws Throwable {
        assertKafka();
        assertServiceAccount();
        assertTopic();

        var bootstrapHost = kafka.bootstrapServerHost;
        var clientID = serviceAccount.clientID;
        var clientSecret = serviceAccount.clientSecret;
        var topicName = topic.name;

        bwait(testTopicPlain(
            vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            topicName,
            1000,
            10,
            100));
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    @Order(3)
    void testFailedPlainMessaging() {
        assertKafka();
        assertServiceAccount();
        assertTopic();

        var bootstrapHost = kafka.bootstrapServerHost;
        var clientID = serviceAccount.clientID;

        assertThrows(KafkaException.class, () -> bwait(testTopicPlain(
            vertx,
            bootstrapHost,
            clientID,
            "invalid",
            TOPIC_NAME,
            1,
            10,
            11)));
    }


    @Test
    @Order(2)
    void testListAndSearchKafkaInstance() throws Throwable {
        assertKafka();

        //List kafka instances
        var kafkaList = bwait(api.getListOfKafkas());

        LOGGER.info("fetch kafka instance list: {}", Json.encode(kafkaList.items));
        assertTrue(kafkaList.items.size() > 0);

        //Get created kafka instance from the list
        List<KafkaResponse> filteredKafka = kafkaList.items.stream().filter(k -> k.name.equals(KAFKA_INSTANCE_NAME)).collect(Collectors.toList());
        LOGGER.info("Filter kafka instance from list: {}", Json.encode(filteredKafka));
        assertEquals(1, filteredKafka.size());

        //Search kafka by name
        var kafkaOptional = bwait(getKafkaByName(api, KAFKA_INSTANCE_NAME));

        var kafka = kafkaOptional.orElseThrow();
        LOGGER.info("Get created kafka instance is : {}", Json.encode(kafka));
        assertEquals(KAFKA_INSTANCE_NAME, kafka.name);
    }

    @Test
    @Order(2)
    void testCreateKafkaInstanceWithExistingName() {
        assertKafka();

        // Create Kafka Instance with existing name
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        kafkaPayload.name = KAFKA_INSTANCE_NAME;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";

        LOGGER.info("create kafka instance with existing name: {}", kafkaPayload.name);
        assertThrows(HTTPConflictException.class, () -> bwait(api.createKafka(kafkaPayload, true)));
    }

    @Test
    @Order(5)
    void testDeleteProvisioningKafkaInstance() throws Throwable {
        assertAPI();

        // TODO: Move this tests in a separate Class

        // Create Kafka Instance
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        // add postfix to the name based on owner
        kafkaPayload.name = KAFKA2_INSTANCE_NAME;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";

        LOGGER.info("create kafka instance: {}", KAFKA2_INSTANCE_NAME);
        var kafkaToDelete = bwait(api.createKafka(kafkaPayload, true));

        LOGGER.info("wait 3 seconds before start deleting");
        bwait(sleep(vertx, ofSeconds(3)));

        LOGGER.info("delete kafka: {}", kafkaToDelete.id);
        bwait(api.deleteKafka(kafkaToDelete.id, true));

        LOGGER.info("wait for kafka to be deleted: {}", kafkaToDelete.id);
        bwait(waitUntilKafkaIsDeleted(vertx, api, kafkaToDelete.id));
    }

    @Test
    @Order(5)
    void testDeleteKafkaInstance() throws Throwable {
        assertKafka();
        assertServiceAccount();
        assertTopic();

        var bootstrapHost = kafka.bootstrapServerHost;
        var clientID = serviceAccount.clientID;
        var clientSecret = serviceAccount.clientSecret;

        // Connect the Kafka producer
        LOGGER.info("initialize kafka producer; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        var producer = KafkaProducerClient.createProducer(vertx, bootstrapHost, clientID, clientSecret);

        // Delete the Kafka instance
        LOGGER.info("Delete kafka instance : {}", KAFKA_INSTANCE_NAME);
        bwait(api.deleteKafka(kafka.id, true));
        bwait(waitUntilKafkaIsDeleted(vertx, api, kafka.id));

        // give it 1s more
        bwait(sleep(vertx, ofSeconds(1)));

        // Produce Kafka messages
        LOGGER.info("send message to topic: {}", TOPIC_NAME);
        //SslAuthenticationException
        assertThrows(Exception.class,
            () -> bwait(producer.send(KafkaProducerRecord.create(TOPIC_NAME, "hello world"))));

        LOGGER.info("close kafka producer and consumer");
        bwait(producer.close());
    }
}
