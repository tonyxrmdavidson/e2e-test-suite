package io.managed.services.test.smoke;

import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.ResponseException;
import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAccount;
import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.client.serviceapi.CreateServiceAccountPayload;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.KafkaListResponse;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.User;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.stream.Collectors;

import static io.managed.services.test.TestUtils.await;
import static io.managed.services.test.TestUtils.waitUntilKafKaGetsReady;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;


@Tag(TestTag.CI)
@Tag(TestTag.SERVICE_API)
@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ServiceAPIDiffOrgUserPermissionTest extends TestBase {

    private static final Logger LOGGER = LogManager.getLogger(ServiceAPITest.class);

    User userOfOrg1, userOfOrg2;
    KeycloakOAuth auth;
    ServiceAPI apiOrg1, apiOrg2;

    String kafkaIDOrg1, kafkaIDOrg2;
    String serviceAccountIDOrg1, serviceAccountIDOrg2;

    @BeforeAll
    void bootstrap(Vertx vertx, VertxTestContext context) {
        this.auth = new KeycloakOAuth(vertx,
                Environment.SSO_REDHAT_KEYCLOAK_URI,
                Environment.SSO_REDHAT_REDIRECT_URI,
                Environment.SSO_REDHAT_REALM,
                Environment.SSO_REDHAT_CLIENT_ID);

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        User userOfOrg1 = await(auth.login(Environment.SSO_TEST_ORG_PRIMARY_USERNAME, Environment.SSO_TEST_ORG_PRIMARY_PASSWORD));
        User userOfOrg2 = await(auth.login(Environment.SSO_USERNAME, Environment.SSO_PASSWORD));
        this.userOfOrg1 = userOfOrg1;
        this.userOfOrg2 = userOfOrg2;
        this.apiOrg1 = new ServiceAPI(vertx, Environment.SERVICE_API_URI, userOfOrg1);
        this.apiOrg2 = new ServiceAPI(vertx, Environment.SERVICE_API_URI, userOfOrg2);

        context.completeNow();
    }

    @AfterAll
    void deleteKafkaInstance() {
        if (kafkaIDOrg1 != null) {
            LOGGER.info("clean kafka instance: {}", kafkaIDOrg1);
            System.out.println(await(apiOrg1.deleteKafka(kafkaIDOrg1, true)));
        }
        if (kafkaIDOrg2 != null) {
            LOGGER.info("clean kafka instance: {}", kafkaIDOrg2);
            await(apiOrg2.deleteKafka(kafkaIDOrg2, true));
        }
    }

    @AfterAll
    void deleteServiceAccount() {
        if (serviceAccountIDOrg1 != null) {
            LOGGER.info("clean service account: {}", serviceAccountIDOrg1);
            await(apiOrg1.deleteServiceAccount(serviceAccountIDOrg1));
        }

        if (serviceAccountIDOrg2 != null) {
            LOGGER.info("clean service account: {}", serviceAccountIDOrg2);
            await(apiOrg2.deleteServiceAccount(serviceAccountIDOrg2));
        }
    }

    /**
     * Create a new Kafka instance in organization 1
     * Test it should be there for user of organization 1
     * And not available for user of another org
     * Also user from another org not allowed to create topic to produce and consume messages
     */
    @Test
    @Timeout(10 * 60 * 1000)
    @Order(1)
    void testCreateAndListKafkaInstance(Vertx vertx, VertxTestContext context) {

        // Create Kafka Instance in org 1
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        // add postfix to the name based on owner
        kafkaPayload.name = "mk-e2e-" + Environment.KAFKA_POSTFIX_NAME;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";

        LOGGER.info("create kafka instance in organisation 1: {}", kafkaPayload.name);
        KafkaResponse kafka = await(apiOrg1.createKafka(kafkaPayload, true));
        kafkaIDOrg1 = kafka.id;

        // Get list of kafka Instance in org 1 and test it should be there
        KafkaListResponse kafkaListInOrg1 = await(apiOrg1.getListOfKafkas());
        LOGGER.info("fetch list of instance in organisation 1 for user 1");
        List<KafkaResponse> kafkaResponsesInOrg1 = kafkaListInOrg1.items.stream().filter(k -> k.id.equals(kafkaIDOrg1)).collect(Collectors.toList());

        assertEquals(1, kafkaResponsesInOrg1.size(), "Kafka is present in the organisation 1");
        LOGGER.info("Kafka {} is visible to Org 1", kafkaIDOrg1);

        // Get list of kafka Instance in org 2 and test it should not be there
        LOGGER.info("fetch list of instance of organisation 2");
        KafkaListResponse kafkaListInOrg2 = await(apiOrg2.getListOfKafkas());
        List<KafkaResponse> kafkaResponsesInOrg2 = kafkaListInOrg2.items.stream().filter(k -> k.id.equals(kafkaIDOrg1)).collect(Collectors.toList());
        assertEquals(0, kafkaResponsesInOrg2.size(), "Kafka is not present in organisation 2");
        LOGGER.info("Kafka {} is not visible to Org 2", kafkaIDOrg1);

        kafka = waitUntilKafKaGetsReady(vertx, apiOrg1, kafkaIDOrg1);

        // Create Service Account of Org 2
        CreateServiceAccountPayload serviceAccountPayload = new CreateServiceAccountPayload();
        serviceAccountPayload.name = "mk-e2e-org2-sa-auto";

        LOGGER.info("create service account in Org 2: {}", serviceAccountPayload.name);
        ServiceAccount serviceAccountOrg2 = await(apiOrg2.createServiceAccount(serviceAccountPayload));
        serviceAccountIDOrg2 = serviceAccountOrg2.id;

        String bootstrapHost = kafka.bootstrapServerHost;
        String clientID = serviceAccountOrg2.clientID;
        String clientSecret = serviceAccountOrg2.clientSecret;

        // Create Kafka topic in Org 1 from Org 2 and it should fail
        // TODO: User service api to create topics when available
        LOGGER.info("initialize kafka admin; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        try {
            KafkaAdmin admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);
            String topicName = "test-topic";
            LOGGER.info("create kafka topic: {}", topicName);
            await(admin.createTopic(topicName));
            LOGGER.error("it should fail");
            fail("user from different organisation is able to create topic which is not expected");
//            // Consume Kafka messages
//            LOGGER.info("initialize kafka consumer; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
//            KafkaConsumer<String, String> consumer = KafkaUtils.createConsumer(vertx, bootstrapHost, clientID, clientSecret);
//
//            Promise<KafkaConsumerRecord<String, String>> receiver = Promise.promise();
//            consumer.handler(receiver::complete);
//
//            LOGGER.info("subscribe to topic: {}", topicName);
//            await(consumer.subscribe(topicName));
//
//            // TODO: Send and receive multiple messages
//
//            // Produce Kafka messages
//            LOGGER.info("initialize kafka producer; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
//            KafkaProducer<String, String> producer = KafkaUtils.createProducer(vertx, bootstrapHost, clientID, clientSecret);
//
//            LOGGER.info("send message to topic: {}", topicName);
//            await(producer.send(KafkaProducerRecord.create(topicName, "hello world")));
//
//            // Wait for the message
//            LOGGER.info("wait for message");
//            KafkaConsumerRecord<String, String> record = await(receiver.future());
//
//            LOGGER.info("received message: {}", record.value());
//            assertEquals("hello world", record.value());
//
//            LOGGER.info("close kafka producer and consumer");
//            await(producer.close());
//            await(consumer.close());
        } catch (Exception e) {
            LOGGER.info("user from different organisation is not allowed to create topic for instance:{}", kafkaIDOrg1);
        }

        context.completeNow();

    }

    /**
     * Create a new Kafka instance in organization 1
     */
    @Test
    @Timeout(10 * 60 * 1000)
    @Order(2)
    void deleteKafkaOfOrg1ByOrg2(Vertx vertx, VertxTestContext context) {
        if (kafkaIDOrg1 != null) {
            LOGGER.info("Delete Instance: {} of Org 1 using user of Org 2",kafkaIDOrg1);
            await(apiOrg2.deleteKafka(kafkaIDOrg1, true).compose(r -> Future.failedFuture("user from different organisation is able to delete instance")).recover(throwable -> {
                if (throwable instanceof ResponseException) {
                    if (((ResponseException) throwable).response.statusCode() == 404) {
                        LOGGER.info("user from different organisation is not allowed to delete instance");
                        return Future.succeededFuture();
                    }
                }
                LOGGER.info("something went wrong");
                return Future.failedFuture(throwable);
            }));
        }
        context.completeNow();

    }
}
