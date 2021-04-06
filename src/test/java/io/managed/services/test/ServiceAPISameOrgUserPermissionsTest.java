package io.managed.services.test;

import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.client.serviceapi.CreateServiceAccountPayload;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
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
import java.util.stream.Collectors;

import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopicWithOauth;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteKafkaByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteServiceAccountByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsReady;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


@Tag(TestTag.SERVICE_API_PERMISSIONS)
@ExtendWith(VertxExtension.class)
@Timeout(value = 5, timeUnit = TimeUnit.MINUTES)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ServiceAPISameOrgUserPermissionsTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPISameOrgUserPermissionsTest.class);

    static final String KAFKA_INSTANCE_NAME = "mk-e2e-sup-" + Environment.KAFKA_POSTFIX_NAME;
    static final String SERVICE_ACCOUNT_NAME = "mk-e2e-sup-sa-" + Environment.KAFKA_POSTFIX_NAME;

    ServiceAPI api1;
    ServiceAPI api2;

    KafkaResponse kafka;

    @BeforeAll
    void bootstrapApi1(Vertx vertx, VertxTestContext context) {
        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        ServiceAPIUtils.serviceAPI(vertx, Environment.SSO_USERNAME, Environment.SSO_PASSWORD)
                .onSuccess(api -> api1 = api)
                .onComplete(context.succeedingThenComplete());
    }

    @BeforeAll
    void bootstrapApi2(Vertx vertx, VertxTestContext context) {
        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_SECONDARY_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        ServiceAPIUtils.serviceAPI(vertx, Environment.SSO_SECONDARY_USERNAME, Environment.SSO_SECONDARY_PASSWORD)
                .onSuccess(api -> api2 = api)
                .onComplete(context.succeedingThenComplete());

    }

    @AfterAll
    void deleteKafkaInstance(VertxTestContext context) {
        deleteKafkaByNameIfExists(api1, KAFKA_INSTANCE_NAME)
                .onComplete(context.succeedingThenComplete());
    }

    @AfterAll
    void deleteServiceAccount(VertxTestContext context) {
        deleteServiceAccountByNameIfExists(api2, SERVICE_ACCOUNT_NAME)
                .onComplete(context.succeedingThenComplete());
    }

    void assertAPI() {
        assumeTrue(api1 != null, "api1 is null because the bootstrap has failed");
        assumeTrue(api2 != null, "api2 is null because the bootstrap has failed");
    }

    void assertKafka() {
        assumeTrue(kafka != null, "kafka is null because the testUser1CreateKafkaInstance has failed to create the Kafka instance");
    }

    @Test
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    @Order(1)
    void testUser1CreateKafkaInstance(Vertx vertx, VertxTestContext context) {
        assertAPI();

        // Create Kafka Instance
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        // add postfix to the name based on owner
        kafkaPayload.name = KAFKA_INSTANCE_NAME;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";

        LOGGER.info("create kafka instance: {}", kafkaPayload.name);
        api1.createKafka(kafkaPayload, true)
                .compose(k -> waitUntilKafkaIsReady(vertx, api1, k.id))
                .onSuccess(k -> kafka = k)
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    void testUser2ListKafkaInstances(VertxTestContext context) {
        assertKafka();

        // Get kafka instance list by another user with same org
        api2.getListOfKafkas()
                .onSuccess(kafkaList -> context.verify(() -> {
                    LOGGER.info("fetch list of kafka instance for another user same org");
                    List<KafkaResponse> filteredKafka = kafkaList.items.stream()
                            .filter(k -> k.id.equals(kafka.id)).collect(Collectors.toList());

                    LOGGER.info("kafka instance response is: {}", Json.encode(filteredKafka));
                    assertEquals(1, filteredKafka.size(), "kafka is not present for another user with same org");
                }))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    @Timeout(value = 3, timeUnit = TimeUnit.MINUTES)
    void testUser2ProduceAndConsumeKafkaMessages(Vertx vertx, VertxTestContext context) {
        assertKafka();

        // Create Service Account by another user
        CreateServiceAccountPayload serviceAccountPayload = new CreateServiceAccountPayload();
        serviceAccountPayload.name = SERVICE_ACCOUNT_NAME;

        LOGGER.info("create service account by another user with same org: {}", serviceAccountPayload.name);
        api2.createServiceAccount(serviceAccountPayload)
                .compose(serviceAccount -> {

                    String bootstrapHost = kafka.bootstrapServerHost;
                    String clientID = serviceAccount.clientID;
                    String clientSecret = serviceAccount.clientSecret;

                    // Create Kafka topic by another user
                    LOGGER.info("initialize kafka admin; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
                    KafkaAdmin admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);

                    String topicName = "test-topic";
                    LOGGER.info("create kafka topic: {}", topicName);
                    return admin.createTopic(topicName)
                            .compose(__ -> testTopicWithOauth(vertx, bootstrapHost, clientID, clientSecret, topicName, 1, 100, 100))

                            .onComplete(__ -> admin.close());
                })

                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(3)
    void testUser2DeleteKafkaInstance(VertxTestContext context) {
        assertKafka();

        // Delete kafka instance by another user with same org
        api2.deleteKafka(kafka.id, true)
                .onComplete(context.failingThenComplete());
    }
}
