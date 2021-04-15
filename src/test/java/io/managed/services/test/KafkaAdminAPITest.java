package io.managed.services.test;

import io.managed.services.test.client.ResponseException;
import io.managed.services.test.client.kafkaadminapi.KafkaAdminAPI;
import io.managed.services.test.client.kafkaadminapi.KafkaAdminAPIUtils;
import io.managed.services.test.client.kafkaadminapi.Topic;
import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
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

import java.net.HttpURLConnection;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsReady;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(TestTag.KAFKA_ADMIN_API)
@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KafkaAdminAPITest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(KafkaAdminAPITest.class);

    KafkaAdminAPI kafkaAdminAPI;
    ServiceAPI serviceAPI;
    KafkaResponse kafka;
    Topic topic;
    String group;

    static final String KAFKA_INSTANCE_NAME = "mk-e2e-kaa-" + Environment.KAFKA_POSTFIX_NAME;
    static final String TEST_TOPIC_NAME = "test-api-topic-1";
    static final String TEST_NOT_EXISTING_TOPIC_NAME = "test-api-topic-not-exist";

    static final String TEST_ACTIVE_GROUP_NAME = "strimzi-canary-group";
    static final String TEST_NOT_EXISTING_GROUP_NAME = "not-existing-group";


    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    @BeforeAll
    void bootstrap(Vertx vertx, VertxTestContext context) {
        ServiceAPIUtils.serviceAPI(vertx)
                .onSuccess(a -> serviceAPI = a)
                .onComplete(context.succeedingThenComplete());
    }


    @AfterAll
    void deleteKafkaInstance(VertxTestContext context) {

        ServiceAPIUtils.deleteKafkaByNameIfExists(serviceAPI, KAFKA_INSTANCE_NAME)
                .onComplete(context.succeedingThenComplete());
    }

    void assertKafkaAdminAPI() {
        assumeTrue(kafkaAdminAPI != null, "kafkaAdminAPI is null because the testConnectKafkaAdminAPI has failed");
    }

    void assertTopic() {
        assumeTrue(topic != null, "topic is null because the testCreateTopic has failed");
    }

    void assertServiceAPI() {
        assumeTrue(serviceAPI != null, "serviceAPI is null because the bootstrap has failed");
    }

    void assertKafka() {
        assumeTrue(kafka != null, "kafka is null because the testCreateKafkaInstance has failed");
    }

    @Test
    @Order(1)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testCreateKafkaInstance(Vertx vertx, VertxTestContext context) {
        assertServiceAPI();

        // Create Kafka Instance
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        // add postfix to the name based on owner
        kafkaPayload.name = KAFKA_INSTANCE_NAME;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";

        LOGGER.info("create kafka instance: {}", kafkaPayload.name);
        serviceAPI.createKafka(kafkaPayload, true)
                .compose(k -> waitUntilKafkaIsReady(vertx, serviceAPI, k.id))
                .onSuccess(k -> kafka = k)
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testConnectKafkaAdminAPI(Vertx vertx, VertxTestContext context) {
        assertServiceAPI();
        assertKafka();

        var bootstrapServerHost = kafka.bootstrapServerHost;

        KafkaAdminAPIUtils.kafkaAdminAPI(vertx, bootstrapServerHost)
                .onSuccess(restApiResponse -> kafkaAdminAPI = restApiResponse)
                .onFailure(msg -> System.out.println(msg.getMessage()))
                .onComplete(context.succeedingThenComplete());

    }

    @Test
    @Order(3)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testCreateTopic(VertxTestContext context) {
        assertKafkaAdminAPI();

        kafkaAdminAPI.getTopicByName(TEST_TOPIC_NAME)
                .compose(r -> Future.failedFuture("getting test-topic should fail because the topic shouldn't exists"))
                .recover(throwable -> {
                    if ((throwable instanceof ResponseException) && (((ResponseException) throwable).response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND)) {
                        LOGGER.info("topic not found : {}", TEST_TOPIC_NAME);
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })
                .compose(a -> {
                    LOGGER.info("create topic: {}", TEST_TOPIC_NAME);
                    return KafkaAdminAPIUtils.createDefaultTopic(kafkaAdminAPI, TEST_TOPIC_NAME);
                })
                .onSuccess(t -> {
                    LOGGER.info("topic created: {}", TEST_TOPIC_NAME);
                    topic = t;
                })
                .onComplete(context.succeedingThenComplete());


    }

    @Test
    @Order(4)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testCreateExistingTopic(VertxTestContext context) {
        assertKafkaAdminAPI();
        assertTopic();

        KafkaAdminAPIUtils.createDefaultTopic(kafkaAdminAPI, TEST_TOPIC_NAME)
                .compose(r -> Future.failedFuture("Create existing topic should fail"))
                .recover(throwable -> {
                    if (throwable instanceof ResponseException) {
                        if (((ResponseException) throwable).response.statusCode() == HttpURLConnection.HTTP_CONFLICT) {
                            LOGGER.info("Existing topic cannot be created again : {}", TEST_TOPIC_NAME);
                            return Future.succeededFuture();
                        }
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testGetTopicByName(VertxTestContext context) {
        assertKafkaAdminAPI();
        assertTopic();

        kafkaAdminAPI.getTopicByName(TEST_TOPIC_NAME)
                .onSuccess(t -> context.verify(() -> assertEquals(TEST_TOPIC_NAME, t.name)))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testGetNotExistingTopicByName(VertxTestContext context) {
        assertKafkaAdminAPI();

        kafkaAdminAPI.getTopicByName(TEST_NOT_EXISTING_TOPIC_NAME)
                .compose(r -> Future.failedFuture("Get none existing topic should fail"))
                .recover(throwable -> {
                    if (throwable instanceof ResponseException) {
                        if (((ResponseException) throwable).response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                            LOGGER.info("Topic not found : {}", TEST_NOT_EXISTING_TOPIC_NAME);
                            return Future.succeededFuture();
                        }
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }


    @Test
    @Order(4)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void tetGetAllTopics(VertxTestContext context) {
        assertKafkaAdminAPI();
        assertTopic();

        kafkaAdminAPI.getAllTopics()
                .onSuccess(topics -> context.verify(() -> {
                    List<Topic> filteredTopics = topics.items.stream()
                            .filter(k -> k.name.equals(TEST_TOPIC_NAME) || k.name.equals("strimzi-canary"))
                            .collect(Collectors.toList());

                    assertEquals(2, filteredTopics.size());
                }))
                .onComplete(context.succeedingThenComplete());
    }


    @Test
    @Order(5)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testDeleteTopicByName(VertxTestContext context) {
        assertKafkaAdminAPI();
        assertTopic();

        kafkaAdminAPI.deleteTopicByName(TEST_TOPIC_NAME)
                .compose(r -> kafkaAdminAPI.getTopicByName(TEST_TOPIC_NAME))
                .compose(r -> Future.failedFuture("Getting test-topic should fail due to topic being deleted in current test"))
                .recover(throwable -> {
                    if ((throwable instanceof ResponseException) && (((ResponseException) throwable).response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND)) {
                        System.out.println(((ResponseException) throwable).response.bodyAsString());
                        LOGGER.info("Topic not found : {}", TEST_TOPIC_NAME);
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());

    }

    @Test
    @Order(5)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testDeleteNotExistingTopicByName(VertxTestContext context) {
        assertKafkaAdminAPI();

        kafkaAdminAPI.deleteTopicByName(TEST_NOT_EXISTING_TOPIC_NAME)
                .compose(__ -> Future.failedFuture("Deleting not existing topic should result in HTTP_NOT_FOUND response"))
                .recover(throwable -> {
                    if ((throwable instanceof ResponseException) && (((ResponseException) throwable).response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND)) {
                        LOGGER.info("Topic not found : {}", TEST_NOT_EXISTING_TOPIC_NAME);
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }

    // TODO: current API version response to delete attempts with 401 response, which may change over time.
    @Test
    @Order(3)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testGetAllGroups(VertxTestContext context) {
        assertKafkaAdminAPI();

        kafkaAdminAPI.getAllGroups()
                .onSuccess(groupResponse -> context.verify(() -> {
                    // TODO: create a consumer and assert that the created consumer group exists
                    int groupsCount = groupResponse.length;
                    assertTrue(groupsCount >= 1);
                }))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(3)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testGetGroupByName(VertxTestContext context) {
        assertKafkaAdminAPI();

        kafkaAdminAPI.getGroupByName(TEST_ACTIVE_GROUP_NAME)
                .onSuccess(groupResponse -> context.verify(() -> {
                    assertEquals(groupResponse.id, TEST_ACTIVE_GROUP_NAME);
                    assertEquals("STABLE", groupResponse.state);
                    group = groupResponse.id;
                }))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(3)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testGetNotExistingGroupByName(VertxTestContext context) {
        assertKafkaAdminAPI();

        kafkaAdminAPI.getGroupByName(TEST_NOT_EXISTING_GROUP_NAME)
                .onSuccess(groupResponse -> context.verify(() -> assertEquals("DEAD", groupResponse.state)))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testDeleteActiveGroupByName(VertxTestContext context) {
        assertKafkaAdminAPI();

        kafkaAdminAPI.deleteGroupByName(TEST_ACTIVE_GROUP_NAME)
                .compose(r -> Future.failedFuture("Deleting existing not empty group by name"))
                .recover(throwable -> {
                    if (throwable instanceof ResponseException) {
                        if (((ResponseException) throwable).response.statusCode() == HttpURLConnection.HTTP_UNAUTHORIZED) {
                            LOGGER.info("Group isn't empty, thus cannot be deleted : {}", TEST_ACTIVE_GROUP_NAME);
                            return Future.succeededFuture();
                        }
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(3)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testDeleteNotExistingGroupByName(VertxTestContext context) {
        assertKafkaAdminAPI();

        kafkaAdminAPI.deleteGroupByName(TEST_NOT_EXISTING_GROUP_NAME)
                .compose(r -> Future.failedFuture("Deleting group by not existing name"))
                .recover(throwable -> {
                    if (throwable instanceof ResponseException) {
                        if (((ResponseException) throwable).response.statusCode() == HttpURLConnection.HTTP_UNAUTHORIZED) {
                            LOGGER.info("Group is not existing thus cannot be deleted: {}", TEST_NOT_EXISTING_TOPIC_NAME);
                            return Future.succeededFuture();
                        }
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }

    // TODO: Tests delete group for real
}