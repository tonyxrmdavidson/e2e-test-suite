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
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.client.serviceapi.ServiceAccount;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.User;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.managed.services.test.TestUtils.await;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteServiceAccountByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.getKafkaByName;
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

    User userOfOrg1, userOfOrg2;
    KeycloakOAuth auth;
    ServiceAPI apiOrg1, apiOrg2;

    boolean kafkaInstanceCreated = false;

    @BeforeAll
    void bootstrap(Vertx vertx, VertxTestContext context) {
        this.auth = new KeycloakOAuth(vertx,
            Environment.SSO_REDHAT_KEYCLOAK_URI,
            Environment.SSO_REDHAT_REDIRECT_URI,
            Environment.SSO_REDHAT_REALM,
            Environment.SSO_REDHAT_CLIENT_ID);

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        User userOfOrg1 = await(auth.login(Environment.SSO_USERNAME, Environment.SSO_PASSWORD));
        User userOfOrg2 = await(auth.login(Environment.SSO_ALIEN_USERNAME, Environment.SSO_ALIEN_PASSWORD));

        this.userOfOrg1 = userOfOrg1;
        this.userOfOrg2 = userOfOrg2;
        this.apiOrg1 = new ServiceAPI(vertx, Environment.SERVICE_API_URI, userOfOrg1);
        this.apiOrg2 = new ServiceAPI(vertx, Environment.SERVICE_API_URI, userOfOrg2);

        context.completeNow();
    }

    @AfterAll
    void deleteKafkaInstance() {
        await(ServiceAPIUtils.deleteKafkaByNameIfExists(apiOrg1, KAFKA_INSTANCE_NAME));
    }

    @AfterAll
    void deleteServiceAccount() {
        await(deleteServiceAccountByNameIfExists(apiOrg2, SERVICE_ACCOUNT_NAME_ORG2));
    }

    /**
     * Create a new Kafka instance in organization 1
     * Test it should be there for user of organization 1
     * And not available for user of another org
     */
    @Test
    @Timeout(10 * 60 * 1000)
    @Order(1)
    void testCreateAndListKafkaInstance(Vertx vertx) {

        // Create Kafka Instance in org 1
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        // add postfix to the name based on owner
        kafkaPayload.name = KAFKA_INSTANCE_NAME;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";

        LOGGER.info("create kafka instance in organisation 1: {}", kafkaPayload.name);
        KafkaResponse kafka = await(apiOrg1.createKafka(kafkaPayload, true));
        String kafkaIDOrg1 = kafka.id;

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

        // Wait until kafka goes to ready state
        waitUntilKafkaIsReady(vertx, apiOrg1, kafkaIDOrg1);

        kafkaInstanceCreated = true;
    }

    /**
     * A user in org A is not allowed to create topic to produce and consume messages on a kafka instance in org B
     */
    @Test
    @Order(2)
    @Disabled("Known issue: https://issues.redhat.com/browse/MGDSTRM-1439")
    @Timeout(value = 5, timeUnit = TimeUnit.MINUTES)
    void testCreateTopicInOrg1KafkaByOrg2() {
        assumeTrue(kafkaInstanceCreated, "testCreateAndListKafkaInstance failed");

        var kafka = await(getKafkaByName(apiOrg1, KAFKA_INSTANCE_NAME)).orElseThrow();

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

    /**
     * Create a new Kafka instance in organization 1
     */
    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    @Order(3)
    void deleteKafkaOfOrg1ByOrg2() {
        assumeTrue(kafkaInstanceCreated, "testCreateAndListKafkaInstance failed");

        var kafka = await(getKafkaByName(apiOrg1, KAFKA_INSTANCE_NAME)).orElseThrow();

        LOGGER.info("Delete Instance: {} of Org 1 using user of Org 2", kafka.id);
        await(apiOrg2.deleteKafka(kafka.id, true)
            .compose(r -> Future.failedFuture("user from different organisation is able to delete instance"))
            .recover(throwable -> {
                if (throwable instanceof ResponseException) {
                    if (((ResponseException) throwable).response.statusCode() == 404) {
                        LOGGER.info("user from different organisation is not allowed to delete instance");
                        return Future.succeededFuture();
                    }
                }
                return Future.failedFuture(throwable);
            }));
    }
}
