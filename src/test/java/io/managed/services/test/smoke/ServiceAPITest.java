package io.managed.services.test.smoke;

import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.ResponseException;
import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.kafka.KafkaUtils;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.client.serviceapi.CreateServiceAccountPayload;
import io.managed.services.test.client.serviceapi.KafkaListResponse;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAccount;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.ext.auth.User;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducer;
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
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.managed.services.test.TestUtils.await;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteKafkaByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteServiceAccountByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.getKafkaByName;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsDelete;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsReady;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


@Tag(TestTag.CI)
@Tag(TestTag.SERVICE_API)
@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ServiceAPITest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPITest.class);

    static final String KAFKA_INSTANCE_NAME = "mk-e2e-" + Environment.KAFKA_POSTFIX_NAME;
    static final String SERVICE_ACCOUNT_NAME = "mk-e2e-sa-" + Environment.KAFKA_POSTFIX_NAME;
    static final String TOPIC_NAME = "test-topic";

    User user;
    KeycloakOAuth auth;
    ServiceAPI api;
    String kafkaId;
    KafkaAdmin admin;
    String bootstrapHost, clientID, clientSecret;

    boolean kafkaInstanceCreated = false;

    @BeforeAll
    void bootstrap(Vertx vertx, VertxTestContext context) {
        this.auth = new KeycloakOAuth(vertx,
                Environment.SSO_REDHAT_KEYCLOAK_URI,
                Environment.SSO_REDHAT_REDIRECT_URI,
                Environment.SSO_REDHAT_REALM,
                Environment.SSO_REDHAT_CLIENT_ID);

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        User user = await(auth.login(Environment.SSO_USERNAME, Environment.SSO_PASSWORD));

        this.user = user;
        this.api = new ServiceAPI(vertx, Environment.SERVICE_API_URI, user);

        context.completeNow();
    }

    @AfterAll
    void deleteKafkaInstance() {
        await(deleteKafkaByNameIfExists(api, KAFKA_INSTANCE_NAME));
    }

    @AfterAll
    void deleteServiceAccount() {
        await(deleteServiceAccountByNameIfExists(api, SERVICE_ACCOUNT_NAME));
    }

    /**
     * Create a new Kafka instance and test that is possible to create topics, send messages and receive messages
     */
    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    @Order(1)
    void testCreateKafkaInstance(Vertx vertx) {

        // Create Kafka Instance
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        // add postfix to the name based on owner
        kafkaPayload.name = KAFKA_INSTANCE_NAME;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";

        LOGGER.info("create kafka instance: {}", kafkaPayload.name);
        KafkaResponse kafka = await(api.createKafka(kafkaPayload, true));
        kafkaId = kafka.id;

        kafka = waitUntilKafkaIsReady(vertx, api, kafka.id);


        // Create Service Account
        CreateServiceAccountPayload serviceAccountPayload = new CreateServiceAccountPayload();
        serviceAccountPayload.name = SERVICE_ACCOUNT_NAME;

        LOGGER.info("create service account: {}", serviceAccountPayload.name);
        ServiceAccount serviceAccount = await(api.createServiceAccount(serviceAccountPayload));

        bootstrapHost = kafka.bootstrapServerHost;
        clientID = serviceAccount.clientID;
        clientSecret = serviceAccount.clientSecret;

        // Create Kafka topic
        // TODO: User service api to create topics when available
        LOGGER.info("initialize kafka admin; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);

        LOGGER.info("create kafka topic: {}", TOPIC_NAME);
        await(admin.createTopic(TOPIC_NAME));

        // Consume Kafka messages
        LOGGER.info("initialize kafka consumer; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        KafkaConsumer<String, String> consumer = KafkaUtils.createConsumer(vertx, bootstrapHost, clientID, clientSecret);

        Promise<KafkaConsumerRecord<String, String>> receiver = Promise.promise();
        consumer.handler(receiver::complete);

        LOGGER.info("subscribe to topic: {}", TOPIC_NAME);
        await(consumer.subscribe(TOPIC_NAME));

        // TODO: Send and receive multiple messages

        // Produce Kafka messages
        LOGGER.info("initialize kafka producer; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        KafkaProducer<String, String> producer = KafkaUtils.createProducer(vertx, bootstrapHost, clientID, clientSecret);

        LOGGER.info("send message to topic: {}", TOPIC_NAME);
        await(producer.send(KafkaProducerRecord.create(TOPIC_NAME, "hello world")));

        // Wait for the message
        LOGGER.info("wait for message");
        KafkaConsumerRecord<String, String> record = await(receiver.future());

        LOGGER.info("received message: {}", record.value());
        assertEquals("hello world", record.value());

        LOGGER.info("close kafka producer and consumer");
        await(producer.close());
        await(consumer.close());

        kafkaInstanceCreated = true;
    }

    @Test
    @Order(2)
    @Timeout(value = 5, timeUnit = TimeUnit.MINUTES)
    void testListAndSearchKafkaInstance() {
        assumeTrue(kafkaInstanceCreated, "testCreateKafkaInstance failed");

        //List kafka instances
        KafkaListResponse kafkaList = await(api.getListOfKafkas());
        LOGGER.info("fetch kafka instance list: {}", Json.encode(kafkaList.items));
        assertTrue(kafkaList.items.size() > 0);

        //Get created kafka instance from the list
        List<KafkaResponse> filteredKafka = kafkaList.items.stream().filter(k -> k.name.equals(KAFKA_INSTANCE_NAME)).collect(Collectors.toList());
        LOGGER.info("Filter kafka instance from list: {}", Json.encode(filteredKafka));
        assertEquals(1, filteredKafka.size());

        //Search kafka by name
        KafkaResponse kafka = await(getKafkaByName(api, KAFKA_INSTANCE_NAME)).orElseThrow();
        LOGGER.info("Get created kafka instance is : {}", Json.encode(kafka));
        assertEquals(KAFKA_INSTANCE_NAME, kafka.name);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.MINUTES)
    @Order(2)
    void testCreateKafkaInstanceWithExistingName() {
        assumeTrue(kafkaInstanceCreated, "testCreateKafkaInstance failed");

        // Create Kafka Instance with existing name
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        kafkaPayload.name = KAFKA_INSTANCE_NAME;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";

        LOGGER.info("create kafka instance with existing name: {}", kafkaPayload.name);

        await(api.createKafka(kafkaPayload, true)
                .compose(r -> Future.failedFuture("create Kafka instance with existing name should fail"))
                .recover(throwable -> {
                    if (throwable instanceof ResponseException) {
                        if (((ResponseException) throwable).response.statusCode() == 409) {
                            LOGGER.info("Existing kafka instance name can't be create : {}", kafkaPayload.name);
                            return Future.succeededFuture();
                        }
                    }
                    return Future.failedFuture(throwable);
                }));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.MINUTES)
    @Order(3)
    void testDeleteTopic() {
        assumeTrue(kafkaInstanceCreated, "testCreateKafkaInstance failed");

        LOGGER.info("Delete created topic : {}", TOPIC_NAME);
        try {
            await(admin.deleteTopic(TOPIC_NAME));
            LOGGER.info("Topic deleted: {}", TOPIC_NAME);
        } catch (CompletionException e) {
            LOGGER.error("{} should be deleted", TOPIC_NAME);
            fail("Created topic should be deleted");
        }
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.MINUTES)
    @Order(4)
    void testVerifyNotToSendAndReceiveMessageAfterDeleteKafkaInstance(Vertx vertx, VertxTestContext context) {
        // Consume Kafka messages
        LOGGER.info("initialize kafka consumer; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        KafkaConsumer<String, String> consumer = KafkaUtils.createConsumer(vertx, bootstrapHost, clientID, clientSecret);

        Promise<KafkaConsumerRecord<String, String>> receiver = Promise.promise();
        consumer.handler(receiver::complete);

        LOGGER.info("subscribe to topic: {}", TOPIC_NAME);
        await(consumer.subscribe(TOPIC_NAME));

        LOGGER.info("initialize kafka producer; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        KafkaProducer<String, String> producer = KafkaUtils.createProducer(vertx, bootstrapHost, clientID, clientSecret);

        LOGGER.info("Delete kafka instance : {}", KAFKA_INSTANCE_NAME);
        waitUntilKafkaIsDelete(vertx, api, kafkaId);

        //Get kafka instance to make sure that kafka has deleted
        await(api.getKafka(kafkaId)
                .compose(r -> {
                    LOGGER.error("Kafka response after deleted kafka: {}", Json.encode(r));
                    return Future.failedFuture("Get kafka request should Ideally failed!");
                })
                .recover(throwable -> {
                    if (throwable instanceof ResponseException) {
                        if (((ResponseException) throwable).response.statusCode() == 404) {
                            LOGGER.info("Kafka instance not found");
                            return Future.succeededFuture();
                        }
                    }
                    return Future.failedFuture(throwable);
                }));


        // Produce Kafka messages
        LOGGER.info("send message to topic: {}", TOPIC_NAME);
        //SslAuthenticationException
        await(producer.send(KafkaProducerRecord.create(TOPIC_NAME, "hello world"))
                .compose(r -> Future.failedFuture("Send message should failed!"))
                .recover(throwable -> {
                    if (throwable instanceof Exception) {
                        LOGGER.info("Send message has failed");
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })
        );

        LOGGER.info("close kafka producer and consumer");
        await(producer.close());
        await(consumer.close());

        context.completeNow();
    }
}
