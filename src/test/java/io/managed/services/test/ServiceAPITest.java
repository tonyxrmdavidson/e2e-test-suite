package io.managed.services.test;

import io.managed.services.test.client.ResponseException;
import io.managed.services.test.client.kafka.KafkaProducerClient;
import io.managed.services.test.client.kafkaadminapi.KafkaAdminAPIUtils;
import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.client.serviceapi.CreateServiceAccountPayload;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.client.serviceapi.ServiceAccount;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.managed.services.test.TestUtils.sleep;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopicPlain;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopicWithOauth;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteKafkaByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteServiceAccountByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.getKafkaByName;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsDeleted;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsReady;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


@Tag(TestTag.SERVICE_API)
@ExtendWith(VertxExtension.class)
@Timeout(value = 5, timeUnit = TimeUnit.MINUTES)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ServiceAPITest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPITest.class);

    static final String KAFKA_INSTANCE_NAME = "mk-e2e-" + Environment.KAFKA_POSTFIX_NAME;
    static final String KAFKA2_INSTANCE_NAME = "mk-e2e-2-" + Environment.KAFKA_POSTFIX_NAME;
    static final String SERVICE_ACCOUNT_NAME = "mk-e2e-sa-" + Environment.KAFKA_POSTFIX_NAME;
    static final String TOPIC_NAME = "test-topic";

    ServiceAPI api;

    KafkaResponse kafka;
    ServiceAccount serviceAccount;
    String topic;

    @BeforeAll
    void bootstrap(Vertx vertx, VertxTestContext context) {
        ServiceAPIUtils.serviceAPI(vertx)
                .onSuccess(a -> api = a)
                .onComplete(context.succeedingThenComplete());
    }

    private Future<Void> deleteKafkaInstance(String instanceName) {
        return deleteKafkaByNameIfExists(api, instanceName);
    }

    private Future<Void> deleteServiceAccount() {
        return deleteServiceAccountByNameIfExists(api, SERVICE_ACCOUNT_NAME);
    }

    @AfterAll
    void teardown(Vertx vertx, VertxTestContext context) {

        // delete kafka instance
        var dk = deleteKafkaInstance(KAFKA_INSTANCE_NAME);
        var dk2 = deleteKafkaInstance(KAFKA2_INSTANCE_NAME);

        // delete service account
        var ds = deleteServiceAccount();

        CompositeFuture.join(dk, dk2, ds)
                .compose(__ -> sleep(vertx, ofSeconds(60)))
                .onComplete(context.succeedingThenComplete());
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
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    @Order(1)
    void testCreateKafkaInstance(Vertx vertx, VertxTestContext context) {
        assertAPI();

        // Create Kafka Instance
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        // add postfix to the name based on owner
        kafkaPayload.name = KAFKA_INSTANCE_NAME;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";

        LOGGER.info("create kafka instance: {}", kafkaPayload.name);
        api.createKafka(kafkaPayload, true)
                .compose(k -> waitUntilKafkaIsReady(vertx, api, k.id))
                .onSuccess(k -> kafka = k)
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(1)
    void testCreateServiceAccount(VertxTestContext context) {
        assertAPI();

        // Create Service Account
        CreateServiceAccountPayload serviceAccountPayload = new CreateServiceAccountPayload();
        serviceAccountPayload.name = SERVICE_ACCOUNT_NAME;

        LOGGER.info("create service account: {}", serviceAccountPayload.name);
        api.createServiceAccount(serviceAccountPayload)
                .onSuccess(s -> serviceAccount = s)
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    void testCreateTopic(Vertx vertx, VertxTestContext context) {
        assertKafka();

        var bootstrapHost = kafka.bootstrapServerHost;

        LOGGER.info("create topic with name {} on the instance: {}", TOPIC_NAME, bootstrapHost);
        KafkaAdminAPIUtils.kafkaAdminAPI(vertx, bootstrapHost)

                .compose(api -> KafkaAdminAPIUtils.createDefaultTopic(api, TOPIC_NAME))
                .onSuccess(__ -> topic = TOPIC_NAME)

                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(3)
    @Timeout(value = 3, timeUnit = TimeUnit.MINUTES)
    void testOAuthMessaging(Vertx vertx, VertxTestContext context) {
        assertKafka();
        assertServiceAccount();
        assertTopic();

        var bootstrapHost = kafka.bootstrapServerHost;
        var clientID = serviceAccount.clientID;
        var clientSecret = serviceAccount.clientSecret;

        testTopicWithOauth(vertx, bootstrapHost, clientID, clientSecret, TOPIC_NAME, 1000, 10, 100)
                .onComplete(context.succeedingThenComplete());
    }


    @Test
    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    @Order(3)
    void testFailedOauthMessaging(Vertx vertx, VertxTestContext context) {
        assertKafka();
        assertServiceAccount();
        assertTopic();

        var bootstrapHost = kafka.bootstrapServerHost;
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
    @Order(3)
    void testPlainMessaging(Vertx vertx, VertxTestContext context) {
        assertKafka();
        assertServiceAccount();
        assertTopic();

        var bootstrapHost = kafka.bootstrapServerHost;
        var clientID = serviceAccount.clientID;
        var clientSecret = serviceAccount.clientSecret;

        testTopicPlain(vertx, bootstrapHost, clientID, clientSecret, TOPIC_NAME, 1000, 10, 100)
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    @Order(3)
    void testFailedPlainMessaging(Vertx vertx, VertxTestContext context) {
        assertKafka();
        assertServiceAccount();
        assertTopic();

        var bootstrapHost = kafka.bootstrapServerHost;
        var clientID = serviceAccount.clientID;

        testTopicPlain(vertx, bootstrapHost, clientID, "invalid", TOPIC_NAME, 1000, 10, 100)
                .onComplete(context.failingThenComplete());
    }


    @Test
    @Order(2)
    void testListAndSearchKafkaInstance(VertxTestContext context) {
        assertKafka();

        //List kafka instances
        api.getListOfKafkas()
                .compose(kafkaList -> {
                    LOGGER.info("fetch kafka instance list: {}", Json.encode(kafkaList.items));
                    assertTrue(kafkaList.items.size() > 0);

                    //Get created kafka instance from the list
                    List<KafkaResponse> filteredKafka = kafkaList.items.stream().filter(k -> k.name.equals(KAFKA_INSTANCE_NAME)).collect(Collectors.toList());
                    LOGGER.info("Filter kafka instance from list: {}", Json.encode(filteredKafka));
                    assertEquals(1, filteredKafka.size());

                    //Search kafka by name
                    return getKafkaByName(api, KAFKA_INSTANCE_NAME);
                })
                .onSuccess(kafkaOptional -> context.verify(() -> {
                    var kafka = kafkaOptional.orElseThrow();
                    LOGGER.info("Get created kafka instance is : {}", Json.encode(kafka));
                    assertEquals(KAFKA_INSTANCE_NAME, kafka.name);
                }))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    void testCreateKafkaInstanceWithExistingName(VertxTestContext context) {
        assertKafka();

        // Create Kafka Instance with existing name
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        kafkaPayload.name = KAFKA_INSTANCE_NAME;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";

        LOGGER.info("create kafka instance with existing name: {}", kafkaPayload.name);
        api.createKafka(kafkaPayload, true)
                .compose(r -> Future.failedFuture("create Kafka instance with existing name should fail"))
                .recover(throwable -> {
                    if (throwable instanceof ResponseException) {
                        if (((ResponseException) throwable).response.statusCode() == 409) {
                            LOGGER.info("Existing kafka instance name can't be create : {}", kafkaPayload.name);
                            return Future.succeededFuture();
                        }
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    void testDeleteProvisioningKafkaInstance(Vertx vertx, VertxTestContext context) {
        assertAPI();

        // Create Kafka Instance
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        // add postfix to the name based on owner
        kafkaPayload.name = KAFKA2_INSTANCE_NAME;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";
        AtomicReference<KafkaResponse> kafkaForDeletion = new AtomicReference<>();

        api.createKafka(kafkaPayload, true)
                .compose(kafkaResponse -> {
                    kafkaForDeletion.set(kafkaResponse);
                    return sleep(vertx, ofSeconds(10));
                })
                .compose(__ -> api.deleteKafka(kafkaForDeletion.get().id, true))
                .compose(__ -> waitUntilKafkaIsDeleted(vertx, api, kafkaForDeletion.get().id))
                .compose(__ -> sleep(vertx, ofSeconds(1)))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    void testDeleteKafkaInstance(Vertx vertx, VertxTestContext context) {
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
        api.deleteKafka(kafka.id, true)
                .compose(__ -> waitUntilKafkaIsDeleted(vertx, api, kafka.id))

                // give it 1s more
                .compose(__ -> sleep(vertx, ofSeconds(1)))

                .compose(__ -> {

                    // Produce Kafka messages
                    LOGGER.info("send message to topic: {}", TOPIC_NAME);
                    //SslAuthenticationException
                    return producer.send(KafkaProducerRecord.create(TOPIC_NAME, "hello world"))
                            .compose(r -> Future.failedFuture("send message should failed"))
                            .recover(throwable -> {
                                if (throwable instanceof Exception) {
                                    LOGGER.info("send message has failed");
                                    return Future.succeededFuture();
                                }
                                return Future.failedFuture(throwable);
                            });
                })

                .compose(__ -> {
                    LOGGER.info("close kafka producer and consumer");
                    return producer.close();
                })

                .onComplete(context.succeedingThenComplete());
    }
}
