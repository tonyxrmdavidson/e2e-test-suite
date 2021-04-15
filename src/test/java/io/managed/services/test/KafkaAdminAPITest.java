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

import java.net.HttpURLConnection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.managed.services.test.TestUtils.message;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.getKafkaByName;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsReady;
import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
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
    void teardown(VertxTestContext context) {
        assumeFalse(Environment.SKIP_TEARDOWN, "skip teardown");

        // delete kafka instance
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

        getKafkaByName(serviceAPI, KAFKA_INSTANCE_NAME)
                .compose(o -> o.map(k -> {
                    LOGGER.warn("kafka instance already exists: {}", Json.encode(k));
                    return succeededFuture(k);

                }).orElseGet(() -> {
                    LOGGER.info("create kafka instance: {}", kafkaPayload.name);
                    return serviceAPI.createKafka(kafkaPayload, true)
                            .compose(k -> waitUntilKafkaIsReady(vertx, serviceAPI, k.id));

                }))
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

        kafkaAdminAPI.getTopic(TEST_TOPIC_NAME)
                .compose(r -> failedFuture("getting test-topic should fail because the topic shouldn't exists"))
                .recover(throwable -> {
                    if ((throwable instanceof ResponseException) && (((ResponseException) throwable).response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND)) {
                        LOGGER.info("topic not found : {}", TEST_TOPIC_NAME);
                        return succeededFuture();
                    }
                    return failedFuture(throwable);
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
                .compose(r -> failedFuture("Create existing topic should fail"))
                .recover(throwable -> {
                    if (throwable instanceof ResponseException) {
                        if (((ResponseException) throwable).response.statusCode() == HttpURLConnection.HTTP_CONFLICT) {
                            LOGGER.info("Existing topic cannot be created again : {}", TEST_TOPIC_NAME);
                            return succeededFuture();
                        }
                    }
                    return failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testGetTopicByName(VertxTestContext context) {
        assertKafkaAdminAPI();
        assertTopic();

        kafkaAdminAPI.getTopic(TEST_TOPIC_NAME)
                .onSuccess(t -> context.verify(() -> assertEquals(TEST_TOPIC_NAME, t.name)))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testGetNotExistingTopic(VertxTestContext context) {
        assertKafkaAdminAPI();

        kafkaAdminAPI.getTopic(TEST_NOT_EXISTING_TOPIC_NAME)
                .compose(r -> failedFuture("Get none existing topic should fail"))
                .recover(throwable -> {
                    if (throwable instanceof ResponseException) {
                        if (((ResponseException) throwable).response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                            LOGGER.info("Topic not found : {}", TEST_NOT_EXISTING_TOPIC_NAME);
                            return succeededFuture();
                        }
                    }
                    return failedFuture(throwable);
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
                    LOGGER.info("topics: {}", Json.encode(topics));
                    List<Topic> filteredTopics = topics.items.stream()
                            .filter(k -> k.name.equals(TEST_TOPIC_NAME))
                            .collect(Collectors.toList());

                    assertEquals(1, filteredTopics.size());
                }))
                .onComplete(context.succeedingThenComplete());
    }


    @Test
    @Order(5)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testDeleteTopic(VertxTestContext context) {
        assertKafkaAdminAPI();
        assertTopic();

        kafkaAdminAPI.deleteTopic(TEST_TOPIC_NAME)
                .compose(r -> kafkaAdminAPI.getTopic(TEST_TOPIC_NAME))
                .compose(r -> failedFuture("Getting test-topic should fail due to topic being deleted in current test"))
                .recover(throwable -> {
                    if ((throwable instanceof ResponseException) && (((ResponseException) throwable).response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND)) {
                        System.out.println(((ResponseException) throwable).response.bodyAsString());
                        LOGGER.info("Topic not found : {}", TEST_TOPIC_NAME);
                        return succeededFuture();
                    }
                    return failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());

    }

    @Test
    @Order(5)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testDeleteNotExistingTopic(VertxTestContext context) {
        assertKafkaAdminAPI();

        kafkaAdminAPI.deleteTopic(TEST_NOT_EXISTING_TOPIC_NAME)
                .compose(__ -> failedFuture("Deleting not existing topic should result in HTTP_NOT_FOUND response"))
                .recover(throwable -> {
                    if ((throwable instanceof ResponseException) && (((ResponseException) throwable).response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND)) {
                        LOGGER.info("Topic not found : {}", TEST_NOT_EXISTING_TOPIC_NAME);
                        return succeededFuture();
                    }
                    return failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }

    // TODO: current API version response to delete attempts with 401 response, which may change over time.
    @Test
    @Order(3)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testGetAllConsumerGroups(VertxTestContext context) {
        assertKafkaAdminAPI();

        kafkaAdminAPI.getAllConsumerGroups()
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
    void testGetConsumerGroup(VertxTestContext context) {
        assertKafkaAdminAPI();

        kafkaAdminAPI.getConsumerGroup(TEST_ACTIVE_GROUP_NAME)
                .onSuccess(consumerGroup -> context.verify(() -> {
                    LOGGER.info("consumer group: {}", Json.encode(consumerGroup));
                    assertEquals(consumerGroup.groupId, TEST_ACTIVE_GROUP_NAME);
                    assertTrue(consumerGroup.consumers.size() > 0);
                }))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(3)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testGetNotExistingConsumerGroup(VertxTestContext context) {
        assertKafkaAdminAPI();

        kafkaAdminAPI.getConsumerGroup(TEST_NOT_EXISTING_GROUP_NAME)
                .compose(
                        r -> failedFuture(message("consumer group '{}' shouldn't exists", TEST_NOT_EXISTING_GROUP_NAME)),
                        t -> {
                            if (t instanceof ResponseException) {
                                if (((ResponseException) t).response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                                    LOGGER.info("consumer group '{}' doesn't exists", TEST_NOT_EXISTING_GROUP_NAME);
                                    return succeededFuture();
                                }
                            }
                            return failedFuture(t);
                        }
                )
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testDeleteActiveConsumerGroup(VertxTestContext context) {
        assertKafkaAdminAPI();

        kafkaAdminAPI.deleteConsumerGroup(TEST_ACTIVE_GROUP_NAME)
                .compose(
                        r -> failedFuture("Deleting existing not empty group by name"),
                        t -> {
                            if (t instanceof ResponseException) {
                                if (((ResponseException) t).response.statusCode() == 423) {
                                    LOGGER.info("Group isn't empty, thus cannot be deleted : {}", TEST_ACTIVE_GROUP_NAME);
                                    return succeededFuture();
                                }
                            }
                            return failedFuture(t);
                        })
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(3)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testDeleteNotExistingConsumerGroup(VertxTestContext context) {
        assertKafkaAdminAPI();

        kafkaAdminAPI.deleteConsumerGroup(TEST_NOT_EXISTING_GROUP_NAME)
                .compose(
                        r -> failedFuture("deleting group by not existing name"),
                        t -> {
                            if (t instanceof ResponseException) {
                                if (((ResponseException) t).response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                                    LOGGER.info("group doesn't exists and therefore it cannot be deleted: {}", TEST_NOT_EXISTING_TOPIC_NAME);
                                    return succeededFuture();
                                }
                            }
                            return failedFuture(t);
                        })
                .onComplete(context.succeedingThenComplete());
    }

    // TODO: Tests delete group for real
}