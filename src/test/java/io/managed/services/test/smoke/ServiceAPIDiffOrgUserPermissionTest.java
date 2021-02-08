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
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;

import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteServiceAccountByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsReady;

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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.managed.services.test.TestUtils.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;


@Tag(TestTag.CI)
@Tag(TestTag.SERVICE_API)
@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ServiceAPIDiffOrgUserPermissionTest extends TestBase {

    private static final Logger LOGGER = LogManager.getLogger(ServiceAPITest.class);

    static final String KAFKA_INSTANCE_NAME = "mk-e2e-sup-" + Environment.KAFKA_POSTFIX_NAME;
    static final String SERVICE_ACCOUNT_NAME_ORG2 = "mk-e2e--sup-sa-" + Environment.KAFKA_POSTFIX_NAME + "2";

    User userOfOrg1, userOfOrg2;
    KeycloakOAuth auth;
    ServiceAPI apiOrg1, apiOrg2;

    String kafkaIDOrg1;
    String serviceAccountIDOrg2;

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
     * Also user from another org not allowed to create topic to produce and consume messages
     */
    @Test
    @Timeout(10 * 60 * 1000)
    @Order(1)
    void testCreateAndListKafkaInstance(Vertx vertx, VertxTestContext context) {

        // Create Kafka Instance in org 1
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        // add postfix to the name based on owner
        kafkaPayload.name = KAFKA_INSTANCE_NAME;
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

        // Wait until kafka goes to ready state
        kafka = waitUntilKafkaIsReady(vertx, apiOrg1, kafkaIDOrg1);

        // Create Service Account of Org 2
        CreateServiceAccountPayload serviceAccountPayload = new CreateServiceAccountPayload();
        serviceAccountPayload.name = SERVICE_ACCOUNT_NAME_ORG2;

        LOGGER.info("create service account in Org 2: {}", serviceAccountPayload.name);
        ServiceAccount serviceAccountOrg2 = await(apiOrg2.createServiceAccount(serviceAccountPayload));
        serviceAccountIDOrg2 = serviceAccountOrg2.id;

        String bootstrapHost = kafka.bootstrapServerHost;
        String clientID = serviceAccountOrg2.clientID;
        String clientSecret = serviceAccountOrg2.clientSecret;

        // Create Kafka topic in Org 1 from Org 2 and it should fail
        // TODO: User service api to create topics when available
        LOGGER.info("initialize kafka admin; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        KafkaAdmin admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);

        String topicName = "test-topic";

        LOGGER.info("create kafka topic: {}", topicName);
        try {
            await(admin.createTopic(topicName));
            LOGGER.error("user from another org is able to create topic or produce or consume messages");
            fail("It should fail");
        } catch (CompletionException e) {
            if (e.getCause() instanceof SaslAuthenticationException) {
                LOGGER.info("user from different organisation is not allowed to create topic for instance:{}", kafkaIDOrg1);

            }
        }

        context.completeNow();

    }

    /**
     * Create a new Kafka instance in organization 1
     */
    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    @Order(2)
    void deleteKafkaOfOrg1ByOrg2(Vertx vertx, VertxTestContext context) {
        if (kafkaIDOrg1 != null) {

            LOGGER.info("Delete Instance: {} of Org 1 using user of Org 2", kafkaIDOrg1);

            await(apiOrg2.deleteKafka(kafkaIDOrg1, true).compose(r -> Future.failedFuture("user from different organisation is able to delete instance")).recover(throwable -> {
                if (throwable instanceof ResponseException) {
                    if (((ResponseException) throwable).response.statusCode() == 404) {
                        LOGGER.info("user from different organisation is not allowed to delete instance");
                        return Future.succeededFuture();
                    }
                }
                return Future.failedFuture(throwable);
            }));

        }

        context.completeNow();

    }
}
