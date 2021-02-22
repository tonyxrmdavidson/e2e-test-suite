package io.managed.services.test.smoke;

import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.ResponseException;
import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.kafka.KafkaUtils;
import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.client.serviceapi.CreateServiceAccountPayload;
import io.managed.services.test.client.serviceapi.KafkaListResponse;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.client.serviceapi.ServiceAccount;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.HttpURLConnection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.managed.services.test.TestUtils.await;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteServiceAccountByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsReady;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


@Tag(TestTag.CI)
@Tag(TestTag.SERVICE_API)
@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ServiceAPIDiffOrgUserPermissionTest extends TestBase {

    private static final Logger LOGGER = LogManager.getLogger(ServiceAPITest.class);

    static final String KAFKA_INSTANCE_NAME = "mk-e2e-dup-" + Environment.KAFKA_POSTFIX_NAME;
    static final String SERVICE_ACCOUNT_NAME_ORG2 = "mk-e2e-dup-sa-" + Environment.KAFKA_POSTFIX_NAME;

    ServiceAPI apiOrg1;
    ServiceAPI apiOrg2;

    // Kafka instance in Org1
    KafkaResponse kafka;

    @BeforeAll
    void bootstrap(Vertx vertx) {
        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        this.apiOrg1 = await(ServiceAPIUtils.serviceAPI(vertx, Environment.SSO_USERNAME, Environment.SSO_PASSWORD));

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_ALIEN_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        this.apiOrg2 = await(ServiceAPIUtils.serviceAPI(vertx, Environment.SSO_ALIEN_USERNAME, Environment.SSO_ALIEN_PASSWORD));
    }

    @AfterAll
    void deleteKafkaInstance() {
        await(ServiceAPIUtils.deleteKafkaByNameIfExists(apiOrg1, KAFKA_INSTANCE_NAME));
    }

    @AfterAll
    void deleteServiceAccount() {
        await(deleteServiceAccountByNameIfExists(apiOrg2, SERVICE_ACCOUNT_NAME_ORG2));
    }

    void assertAPI() {
        assumeTrue(apiOrg1 != null, "apiOrg1 is null because the bootstrap has failed");
        assumeTrue(apiOrg2 != null, "apiOrg2 is null because the bootstrap has failed");
    }

    void assertKafka() {
        assumeTrue(kafka != null, "kafka is null because the testUser1CreateKafkaInstance has failed to create the Kafka instance");
    }


    @Test
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    @Order(1)
    void testOrg1UserCreateKafkaInstance(Vertx vertx) {
        assertAPI();

        // Create Kafka Instance in org 1
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        // add postfix to the name based on owner
        kafkaPayload.name = KAFKA_INSTANCE_NAME;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";

        LOGGER.info("create kafka instance in organisation 1: {}", kafkaPayload.name);
        KafkaResponse k = await(apiOrg1.createKafka(kafkaPayload, true));

        // Wait until kafka goes to ready state
        kafka = await(waitUntilKafkaIsReady(vertx, apiOrg1, k.id));
    }

    @Test
    @Order(2)
    void testOrg2UserListKafkaInstances() {
        assertKafka();

        // Get list of kafka Instance in org 1 and test it should be there
        KafkaListResponse kafkaListInOrg1 = await(apiOrg1.getListOfKafkas());
        LOGGER.info("fetch list of instance in organisation 1 for user 1");
        List<KafkaResponse> kafkaResponsesInOrg1 = kafkaListInOrg1.items.stream()
                .filter(k -> k.id.equals(kafka.id)).collect(Collectors.toList());

        assertEquals(1, kafkaResponsesInOrg1.size(), "Kafka is present in the organisation 1");
        LOGGER.info("Kafka {} is visible to Org1 User", kafka.id);

        // Get list of kafka Instance in org 2 and test it should not be there
        LOGGER.info("fetch list of instance of organisation 2");
        KafkaListResponse kafkaListInOrg2 = await(apiOrg2.getListOfKafkas());
        List<KafkaResponse> kafkaResponsesInOrg2 = kafkaListInOrg2.items.stream()
                .filter(k -> k.id.equals(kafka.id)).collect(Collectors.toList());

        assertEquals(0, kafkaResponsesInOrg2.size(), "Kafka is not present in organisation 2");
        LOGGER.info("Kafka {} is not visible to Org2 User", kafka.id);
    }

    /**
     * A user in org A is not allowed to create topic to produce and consume messages on a kafka instance in org B
     */
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.MINUTES)
    @Order(2)
    @Disabled("Known issue: https://issues.redhat.com/browse/MGDSTRM-1439")
    void testOrg2UserCreateTopic() {
        assertKafka();

        // Create Service Account of Org 2
        CreateServiceAccountPayload serviceAccountPayload = new CreateServiceAccountPayload();
        serviceAccountPayload.name = SERVICE_ACCOUNT_NAME_ORG2;

        LOGGER.info("create service account in Org 2: {}", serviceAccountPayload.name);
        ServiceAccount serviceAccountOrg2 = await(apiOrg2.createServiceAccount(serviceAccountPayload));

        String bootstrapHost = kafka.bootstrapServerHost;
        String clientID = serviceAccountOrg2.clientID;
        String clientSecret = serviceAccountOrg2.clientSecret;

        // Create Kafka topic in Org 1 from Org 2 and it should fail
        // TODO: User service api to create topics when available
        LOGGER.info("initialize kafka admin; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        KafkaAdmin admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);

        String topicName = "test-topic";

        LOGGER.info("create kafka topic: {}", topicName);
        await(KafkaUtils.toVertxFuture(admin.createTopic(topicName))
                // convert a success into a failure
                .compose(r -> Future.failedFuture("user from another org is able to create topic or produce or consume messages"))
                .recover(t -> {
                    // convert only the SaslAuthenticationException in a success
                    if (t instanceof SaslAuthenticationException) {
                        LOGGER.info("user from different organisation is not allowed to create topic for instance: {}", kafka.id);
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(t);
                }));
    }


    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    @Order(3)
    void testOrg2UserDeleteKafkaInstance() {
        assertKafka();

        LOGGER.info("Delete Instance: {} of Org 1 using user of Org 2", kafka.id);
        await(apiOrg2.deleteKafka(kafka.id, true)
                .compose(r -> Future.failedFuture("user from different organisation is able to delete instance"))
                .recover(throwable -> {
                    if (throwable instanceof ResponseException) {
                        if (((ResponseException) throwable).response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                            LOGGER.info("user from different organisation is not allowed to delete instance");
                            return Future.succeededFuture();
                        }
                    }
                    return Future.failedFuture(throwable);
                }));
    }
}
