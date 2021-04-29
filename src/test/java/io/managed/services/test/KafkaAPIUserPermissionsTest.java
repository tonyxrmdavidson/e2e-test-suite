package io.managed.services.test;

import io.managed.services.test.client.exception.HTTPNotFoundException;
import io.managed.services.test.client.exception.HTTPUnauthorizedException;
import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.kafkaadminapi.KafkaAdminAPIUtils;
import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.client.serviceapi.CreateServiceAccountPayload;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import org.apache.kafka.common.errors.SaslAuthenticationException;
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

import java.util.concurrent.TimeUnit;

import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.message;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteKafkaByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteServiceAccountByNameIfExists;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


@Tag(TestTag.SERVICE_API_PERMISSIONS)
@Timeout(value = 5, unit = TimeUnit.MINUTES)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KafkaAPIUserPermissionsTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(KafkaAPIUserPermissionsTest.class);

    private static final String KAFKA_INSTANCE_NAME = "mk-e2e-up-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String SECONDARY_SERVICE_ACCOUNT_NAME = "mk-e2e-up-secondary-sa-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String ALIEN_SERVICE_ACCOUNT_NAME = "mk-e2e-up-alien-sa-" + Environment.KAFKA_POSTFIX_NAME;

    private final Vertx vertx = Vertx.vertx();

    private ServiceAPI mainAPI;
    private ServiceAPI secondaryAPI;
    private ServiceAPI alienAPI;
    private KafkaResponse kafka;

    @BeforeAll
    void bootstrap() throws Throwable {

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        mainAPI = bwait(ServiceAPIUtils.serviceAPI(vertx, Environment.SSO_USERNAME, Environment.SSO_PASSWORD));

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_SECONDARY_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        secondaryAPI = bwait(ServiceAPIUtils.serviceAPI(vertx, Environment.SSO_SECONDARY_USERNAME, Environment.SSO_SECONDARY_PASSWORD));

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_ALIEN_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        alienAPI = bwait(ServiceAPIUtils.serviceAPI(vertx, Environment.SSO_ALIEN_USERNAME, Environment.SSO_ALIEN_PASSWORD));
    }

    @AfterAll
    void teardown() {
        assumeFalse(Environment.SKIP_TEARDOWN, "skip teardown");

        try {
            bwait(deleteKafkaByNameIfExists(mainAPI, KAFKA_INSTANCE_NAME));
        } catch (Throwable t) {
            LOGGER.error("clan kafka error: ", t);
        }

        try {
            bwait(deleteServiceAccountByNameIfExists(secondaryAPI, SECONDARY_SERVICE_ACCOUNT_NAME));
        } catch (Throwable t) {
            LOGGER.error("clean secondary service account error: ", t);
        }

        try {
            bwait(deleteServiceAccountByNameIfExists(alienAPI, ALIEN_SERVICE_ACCOUNT_NAME));
        } catch (Throwable t) {
            LOGGER.error("clean alien service account error: ", t);
        }
    }

    void assertAPI() {
        assumeTrue(mainAPI != null, "mainAPI is null because the bootstrap has failed");
        assumeTrue(secondaryAPI != null, "secondaryAPI is null because the bootstrap has failed");
        assumeTrue(alienAPI != null, "alienAPI is null because the bootstrap has failed");
    }

    void assertKafka() {
        assumeTrue(kafka != null, "kafka is null because the testMainUserCreateKafkaInstance has failed to create the Kafka instance");
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    @Order(1)
    void testMainUserCreateKafkaInstance() throws Throwable {
        assertAPI();

        // Create Kafka Instance
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        // add postfix to the name based on owner
        kafkaPayload.name = KAFKA_INSTANCE_NAME;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";


        LOGGER.info("create kafka instance: {}", kafkaPayload.name);
        kafka = bwait(ServiceAPIUtils.applyKafkaInstance(vertx, mainAPI, kafkaPayload));
    }

    @Test
    @Order(2)
    void testSecondaryUserListKafkaInstances() throws Throwable {
        assertKafka();

        // Get kafka instance list by another user with same org
        LOGGER.info("fetch list of kafka instance from the secondary user in the same org");
        var kafkas = bwait(secondaryAPI.getListOfKafkas());

        var o = kafkas.items.stream()
            .filter(k -> k.name.equals(KAFKA_INSTANCE_NAME))
            .findAny();

        assertTrue(o.isPresent(), message("main user kafka is not visible for secondary user; kafkas: {}", Json.encode(kafkas.items)));
    }


    @Test
    @Order(2)
    void testAlienUserListKafkaInstances() throws Throwable {
        assertKafka();

        // Get list of kafka Instance in org 1 and test it should be there
        LOGGER.info("fetch list of kafka instance from the alin user in a different org");
        var kafkas = bwait(alienAPI.getListOfKafkas());

        var o = kafkas.items.stream()
            .filter(k -> k.name.equals(KAFKA_INSTANCE_NAME))
            .findAny();

        assertTrue(o.isEmpty(), message("main user kafka is visible for alien user; kafkas: {}", Json.encode(kafkas.items)));
    }

    /**
     * Use the secondary user to create a service account and consume and produce messages on the
     * kafka instance created by the main user
     */
    @Test
    @Order(2)
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void testSecondaryUserCreateTopicUsingKafkaBin() throws Throwable {
        assertKafka();

        // Create Service Account by another user
        CreateServiceAccountPayload serviceAccountPayload = new CreateServiceAccountPayload();
        serviceAccountPayload.name = SECONDARY_SERVICE_ACCOUNT_NAME;

        LOGGER.info("create service account by another user with same org: {}", serviceAccountPayload.name);
        var serviceAccount = bwait(secondaryAPI.createServiceAccount(serviceAccountPayload));

        String bootstrapHost = kafka.bootstrapServerHost;
        String clientID = serviceAccount.clientID;
        String clientSecret = serviceAccount.clientSecret;

        // Create Kafka topic by another user
        LOGGER.info("initialize kafka admin; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        KafkaAdmin admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);

        String topicName = "test-topic";

        LOGGER.info("create kafka topic: {}", topicName);
        assertThrows(SaslAuthenticationException.class, () -> bwait(admin.createTopic(topicName)));

        admin.close();
    }

    @Test
    @Order(2)
    void testAlienUserCreateTopicUsingKafkaAdminAPI() throws Throwable {
        assertKafka();

        LOGGER.info("initialize kafka admin api for kafka instance: {}", kafka.bootstrapServerHost);
        var admin = bwait(KafkaAdminAPIUtils.kafkaAdminAPI(vertx, kafka.bootstrapServerHost, Environment.SSO_ALIEN_USERNAME, Environment.SSO_ALIEN_PASSWORD));

        var topicName = "api-alien-test-topic";

        LOGGER.info("create kafka topic: {}", topicName);
        assertThrows(HTTPUnauthorizedException.class, () -> bwait(KafkaAdminAPIUtils.createDefaultTopic(admin, topicName)));
    }

    /**
     * A user in org A is not allowed to create topic to produce and consume messages on a kafka instance in org B
     */
    @Test
    @Order(2)
    void testAlienUserCreateTopicUsingKafkaBin() throws Throwable {
        assertKafka();

        // Create Service Account of Org 2
        var serviceAccountPayload = new CreateServiceAccountPayload();
        serviceAccountPayload.name = ALIEN_SERVICE_ACCOUNT_NAME;

        LOGGER.info("create service account in alien org: {}", serviceAccountPayload.name);
        var serviceAccountOrg2 = bwait(alienAPI.createServiceAccount(serviceAccountPayload));


        String bootstrapHost = kafka.bootstrapServerHost;
        String clientID = serviceAccountOrg2.clientID;
        String clientSecret = serviceAccountOrg2.clientSecret;

        // Create Kafka topic in Org 1 from Org 2 and it should fail
        LOGGER.info("initialize kafka admin; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        KafkaAdmin admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);

        String topicName = "alien-test-topic";

        LOGGER.info("create kafka topic: {}", topicName);
        assertThrows(SaslAuthenticationException.class, () -> bwait(admin.createTopic(topicName)));
    }


    @Test
    @Order(3)
    void testSecondaryUserDeleteKafkaInstance() {
        assertKafka();

        // should fail
        assertThrows(HTTPNotFoundException.class, () -> bwait(secondaryAPI.deleteKafka(kafka.id, true)));
    }

    @Test
    @Order(4)
    void testAlienUserDeleteKafkaInstance() {
        assertKafka();

        // should fail
        assertThrows(HTTPNotFoundException.class, () -> bwait(alienAPI.deleteKafka(kafka.id, true)));
    }
}
