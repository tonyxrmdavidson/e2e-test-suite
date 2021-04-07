package io.managed.services.test;

import io.managed.services.test.client.ResponseException;
import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.kafkaadminapi.KafkaAdminAPIUtils;
import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.client.serviceapi.CreateServiceAccountPayload;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
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
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.HttpURLConnection;
import java.util.concurrent.TimeUnit;

import static io.managed.services.test.TestUtils.message;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteKafkaByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteServiceAccountByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsReady;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


@Tag(TestTag.SERVICE_API_PERMISSIONS)
@ExtendWith(VertxExtension.class)
@Timeout(value = 5, timeUnit = TimeUnit.MINUTES)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ServiceAPIUserPermissionsTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPIUserPermissionsTest.class);

    static final String KAFKA_INSTANCE_NAME = "mk-e2e-up-" + Environment.KAFKA_POSTFIX_NAME;
    static final String SECONDARY_SERVICE_ACCOUNT_NAME = "mk-e2e-up-secondary-sa-" + Environment.KAFKA_POSTFIX_NAME;
    static final String ALIEN_SERVICE_ACCOUNT_NAME = "mk-e2e-up-alien-sa-" + Environment.KAFKA_POSTFIX_NAME;

    ServiceAPI mainAPI;
    ServiceAPI secondaryAPI;
    ServiceAPI alienAPI;

    KafkaResponse kafka;

    @BeforeAll
    void bootstrap(Vertx vertx, VertxTestContext context) {

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        var f1 = ServiceAPIUtils.serviceAPI(vertx, Environment.SSO_USERNAME, Environment.SSO_PASSWORD)
                .onSuccess(a -> mainAPI = a);

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_SECONDARY_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        var f3 = ServiceAPIUtils.serviceAPI(vertx, Environment.SSO_SECONDARY_USERNAME, Environment.SSO_SECONDARY_PASSWORD)
                .onSuccess(a -> secondaryAPI = a);

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_ALIEN_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        var f2 = ServiceAPIUtils.serviceAPI(vertx, Environment.SSO_ALIEN_USERNAME, Environment.SSO_ALIEN_PASSWORD)
                .onSuccess(a -> alienAPI = a);


        CompositeFuture.join(f1, f2, f3)
                .onComplete(context.succeedingThenComplete());
    }

    @AfterAll
    void teardown(VertxTestContext context) {
        var f1 = deleteKafkaByNameIfExists(mainAPI, KAFKA_INSTANCE_NAME);

        var f2 = deleteServiceAccountByNameIfExists(secondaryAPI, SECONDARY_SERVICE_ACCOUNT_NAME);
        var f3 = deleteServiceAccountByNameIfExists(alienAPI, ALIEN_SERVICE_ACCOUNT_NAME);

        CompositeFuture.join(f1, f2, f3)
                .onComplete(context.succeedingThenComplete());
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
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    @Order(1)
    void testMainUserCreateKafkaInstance(Vertx vertx, VertxTestContext context) {
        assertAPI();

        // Create Kafka Instance
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        // add postfix to the name based on owner
        kafkaPayload.name = KAFKA_INSTANCE_NAME;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";

        LOGGER.info("create kafka instance: {}", kafkaPayload.name);
        mainAPI.createKafka(kafkaPayload, true)
                .compose(k -> waitUntilKafkaIsReady(vertx, mainAPI, k.id))
                .onSuccess(k -> kafka = k)
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    void testSecondaryUserListKafkaInstances(VertxTestContext context) {
        assertKafka();

        // Get kafka instance list by another user with same org
        LOGGER.info("fetch list of kafka instance from the secondary user in the same org");
        secondaryAPI.getListOfKafkas()
                .onSuccess(kafkas -> context.verify(() -> {
                    var o = kafkas.items.stream()
                            .filter(k -> k.name.equals(KAFKA_INSTANCE_NAME))
                            .findAny();

                    assertTrue(o.isPresent(), message("main user kafka is not visible for secondary user; kafkas: {}", Json.encode(kafkas.items)));
                }))
                .onComplete(context.succeedingThenComplete());
    }


    @Test
    @Order(2)
    void testAlienUserListKafkaInstances(VertxTestContext context) {
        assertKafka();

        // Get list of kafka Instance in org 1 and test it should be there
        LOGGER.info("fetch list of kafka instance from the alin user in a different org");
        alienAPI.getListOfKafkas()
                .onSuccess(kafkas -> context.verify(() -> {
                    var o = kafkas.items.stream()
                            .filter(k -> k.name.equals(KAFKA_INSTANCE_NAME))
                            .findAny();

                    assertTrue(o.isEmpty(), message("main user kafka is visible for alien user; kafkas: {}", Json.encode(kafkas.items)));
                }))

                .onComplete(context.succeedingThenComplete());
    }

    /**
     * Use the secondary user to create a service account and consume and produce messages on the
     * kafka instance created by the main user
     */
    @Test
    @Order(2)
    @Timeout(value = 3, timeUnit = TimeUnit.MINUTES)
    void testSecondaryUserCreateTopicUsingKafkaBin(Vertx vertx, VertxTestContext context) {
        assertKafka();

        // Create Service Account by another user
        CreateServiceAccountPayload serviceAccountPayload = new CreateServiceAccountPayload();
        serviceAccountPayload.name = SECONDARY_SERVICE_ACCOUNT_NAME;

        LOGGER.info("create service account by another user with same org: {}", serviceAccountPayload.name);
        secondaryAPI.createServiceAccount(serviceAccountPayload)
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
                            // convert a success into a failure
                            .compose(__ -> Future.failedFuture("secondary user shouldn't be allow to create a topic on the main user kafka"), t -> {
                                // convert only the SaslAuthenticationException in a success
                                if (t instanceof SaslAuthenticationException) {
                                    LOGGER.info("the secondary user is not allowed to create topic on the main user kafka");
                                    return Future.succeededFuture();
                                }
                                return Future.failedFuture(t);
                            })
                            .onComplete(__ -> admin.close());
                })

                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    void testAlienUserCreateTopicUsingKafkaAdminAPI(Vertx vertx, VertxTestContext context) {
        assertKafka();

        LOGGER.info("initialize kafka admin api for kafka instance: {}", kafka.bootstrapServerHost);
        KafkaAdminAPIUtils.kafkaAdminAPI(vertx, kafka.bootstrapServerHost, Environment.SSO_ALIEN_USERNAME, Environment.SSO_ALIEN_PASSWORD)
                .compose(admin -> {

                    String topicName = "api-alien-test-topic";

                    LOGGER.info("create kafka topic: {}", topicName);
                    return KafkaAdminAPIUtils.createDefaultTopic(admin, topicName)
                            // convert a success into a failure
                            .compose(__ -> Future.failedFuture("alien user shouldn't be allow to create a topic on the main user kafka"), t -> {
                                if (t instanceof ResponseException) {
                                    if (((ResponseException) t).response.statusCode() == HttpURLConnection.HTTP_UNAUTHORIZED) {
                                        LOGGER.info("the alien user is not allowed to create topic on the main user kafka");
                                        return Future.succeededFuture();
                                    }
                                }
                                return Future.failedFuture(t);
                            });
                })
                .onComplete(context.succeedingThenComplete());
    }

    /**
     * A user in org A is not allowed to create topic to produce and consume messages on a kafka instance in org B
     */
    @Test
    @Order(2)
    void testAlienUserCreateTopicUsingKafkaBin(VertxTestContext context) {
        assertKafka();

        // Create Service Account of Org 2
        var serviceAccountPayload = new CreateServiceAccountPayload();
        serviceAccountPayload.name = ALIEN_SERVICE_ACCOUNT_NAME;

        LOGGER.info("create service account in alien org: {}", serviceAccountPayload.name);
        alienAPI.createServiceAccount(serviceAccountPayload)
                .compose(serviceAccountOrg2 -> {

                    String bootstrapHost = kafka.bootstrapServerHost;
                    String clientID = serviceAccountOrg2.clientID;
                    String clientSecret = serviceAccountOrg2.clientSecret;

                    // Create Kafka topic in Org 1 from Org 2 and it should fail
                    LOGGER.info("initialize kafka admin; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
                    KafkaAdmin admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);

                    String topicName = "alien-test-topic";

                    LOGGER.info("create kafka topic: {}", topicName);
                    return admin.createTopic(topicName)
                            // convert a success into a failure
                            .compose(__ -> Future.failedFuture("alien user shouldn't be allow to create a topic on the main user kafka"), t -> {
                                // convert only the SaslAuthenticationException in a success
                                if (t instanceof SaslAuthenticationException) {
                                    LOGGER.info("the alien user is not allowed to create topic on the main user kafka");
                                    return Future.succeededFuture();
                                }
                                return Future.failedFuture(t);
                            })
                            .onComplete(__ -> admin.close());
                })
                .onComplete(context.succeedingThenComplete());
    }


    @Test
    @Order(3)
    void testSecondaryUserDeleteKafkaInstance(VertxTestContext context) {
        assertKafka();

        // should fail
        secondaryAPI.deleteKafka(kafka.id, true)
                .compose(__ -> Future.failedFuture("the secondary user shouldn't be allow to the delete the main user kafka"), t -> {
                    if (t instanceof ResponseException) {
                        if (((ResponseException) t).response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                            LOGGER.info("the secondary user is not allow to delete the main user kafka");
                            return Future.succeededFuture();
                        }
                    }
                    return Future.failedFuture(t);
                })
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    void testAlienUserDeleteKafkaInstance(VertxTestContext context) {
        assertKafka();

        // should fail
        alienAPI.deleteKafka(kafka.id, true)
                .compose(__ -> Future.failedFuture("the alien user shouldn't be allow to delete the main user kafka"), t -> {
                    if (t instanceof ResponseException) {
                        if (((ResponseException) t).response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                            LOGGER.info("the alien user is not allowed to delete the main user kafka");
                            return Future.succeededFuture();
                        }
                    }
                    return Future.failedFuture(t);
                })
                .onComplete(context.succeedingThenComplete());
    }
}
