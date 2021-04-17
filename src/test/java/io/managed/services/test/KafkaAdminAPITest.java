package io.managed.services.test;

import io.managed.services.test.client.exception.HTTPConflictException;
import io.managed.services.test.client.exception.HTTPLockedException;
import io.managed.services.test.client.exception.HTTPNotFoundException;
import io.managed.services.test.client.kafka.KafkaConsumerClient;
import io.managed.services.test.client.kafkaadminapi.KafkaAdminAPI;
import io.managed.services.test.client.kafkaadminapi.KafkaAdminAPIUtils;
import io.managed.services.test.client.kafkaadminapi.Topic;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.managed.services.test.TestUtils.await;
import static io.managed.services.test.TestUtils.message;
import static io.managed.services.test.TestUtils.waitFor;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.applyKafkaInstance;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(TestTag.KAFKA_ADMIN_API)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Timeout(value = 3, unit = TimeUnit.MINUTES)
public class KafkaAdminAPITest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(KafkaAdminAPITest.class);

    Vertx vertx = Vertx.vertx();

    KafkaAdminAPI kafkaAdminAPI;
    ServiceAPI serviceAPI;
    KafkaResponse kafka;
    Topic topic;
    KafkaConsumer<String, String> kafkaConsumer;

    static final String KAFKA_INSTANCE_NAME = "mk-e2e-kaa-" + Environment.KAFKA_POSTFIX_NAME;
    static final String SERVICE_ACCOUNT_NAME = "mk-e2e-kaa-sa-" + Environment.KAFKA_POSTFIX_NAME;
    static final String TEST_TOPIC_NAME = "test-api-topic-1";
    static final String TEST_NOT_EXISTING_TOPIC_NAME = "test-api-topic-not-exist";

    static final String TEST_GROUP_NAME = "test-consumer-group";
    static final String TEST_NOT_EXISTING_GROUP_NAME = "not-existing-group";


    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @BeforeAll
    void bootstrap() throws Throwable {
        serviceAPI = await(ServiceAPIUtils.serviceAPI(vertx));
        LOGGER.info("service api initialized");

        kafka = await(applyKafkaInstance(vertx, serviceAPI, KAFKA_INSTANCE_NAME));
        LOGGER.info("kafka instance created: {}", Json.encode(kafka));
    }

    @AfterAll
    void teardown() throws Throwable {
        assumeFalse(Environment.SKIP_TEARDOWN, "skip teardown");

        // delete kafka instance
        try {
            await(ServiceAPIUtils.deleteKafkaByNameIfExists(serviceAPI, KAFKA_INSTANCE_NAME));
        } catch (Throwable t) {
            LOGGER.error("failed to clean kafka instance: ", t);
        }

        // delete service account
        try {
            await(ServiceAPIUtils.deleteServiceAccountByNameIfExists(serviceAPI, SERVICE_ACCOUNT_NAME));
        } catch (Throwable t) {
            LOGGER.error("failed to clean service account: ", t);
        }
    }

    void assertKafkaAdminAPI() {
        assumeTrue(kafkaAdminAPI != null, "kafkaAdminAPI is null because testConnectKafkaAdminAPI has failed");
    }

    void assertTopic() {
        assumeTrue(topic != null, "topic is null because testCreateTopic has failed");
    }

    void assertServiceAPI() {
        assumeTrue(serviceAPI != null, "serviceAPI is null because the bootstrap has failed");
    }

    void assertKafka() {
        assumeTrue(kafka != null, "kafka is null because the bootstrap has failed");
    }

    void assertConsumerGroup() {
        assumeTrue(kafkaConsumer != null, "kafkaConsumer is null because ");
    }

    @Test
    @Order(1)
    void testConnectKafkaAdminAPI() throws Throwable {
        assertServiceAPI();
        assertKafka();

        var bootstrapServerHost = kafka.bootstrapServerHost;
        kafkaAdminAPI = await(KafkaAdminAPIUtils.kafkaAdminAPI(vertx, bootstrapServerHost));
    }

    @Test
    @Order(2)
    void testCreateTopic() throws Throwable {
        assertKafkaAdminAPI();

        assertThrows(HTTPNotFoundException.class,
            () -> await(kafkaAdminAPI.getTopic(TEST_TOPIC_NAME)),
            "getting test-topic should fail because the topic shouldn't exists");
        LOGGER.info("topic not found : {}", TEST_TOPIC_NAME);

        LOGGER.info("create topic: {}", TEST_TOPIC_NAME);
        topic = await(KafkaAdminAPIUtils.createDefaultTopic(kafkaAdminAPI, TEST_TOPIC_NAME));

        LOGGER.info("topic created: {}", TEST_TOPIC_NAME);
    }

    @Test
    @Order(4)
    void testCreateExistingTopic() {
        assertKafkaAdminAPI();
        assertTopic();

        assertThrows(HTTPConflictException.class,
            () -> await(KafkaAdminAPIUtils.createDefaultTopic(kafkaAdminAPI, TEST_TOPIC_NAME)),
            "create existing topic should fail");

        LOGGER.info("existing topic cannot be created again : {}", TEST_TOPIC_NAME);
    }

    @Test
    @Order(4)
    void testGetTopicByName() throws Throwable {
        assertKafkaAdminAPI();
        assertTopic();

        var t = await(kafkaAdminAPI.getTopic(TEST_TOPIC_NAME));
        LOGGER.info("topic retrieved: {}", Json.encode(t));

        assertEquals(TEST_TOPIC_NAME, t.name);
    }

    @Test
    @Order(4)
    void testGetNotExistingTopic() {
        assertKafkaAdminAPI();

        assertThrows(HTTPNotFoundException.class,
            () -> await(kafkaAdminAPI.getTopic(TEST_NOT_EXISTING_TOPIC_NAME)),
            "get none existing topic should fail");

        LOGGER.info("topic not found : {}", TEST_NOT_EXISTING_TOPIC_NAME);
    }

    @Test
    @Order(4)
    void tetGetAllTopics() throws Throwable {
        assertKafkaAdminAPI();
        assertTopic();

        var topics = await(kafkaAdminAPI.getAllTopics());
        LOGGER.info("topics: {}", Json.encode(topics));
        List<Topic> filteredTopics = topics.items.stream()
            .filter(k -> k.name.equals(TEST_TOPIC_NAME))
            .collect(Collectors.toList());

        assertEquals(1, filteredTopics.size());
    }


    @Test
    @Order(6)
    void testDeleteTopic() throws Throwable {
        assertKafkaAdminAPI();
        assertTopic();

        await(kafkaAdminAPI.deleteTopic(TEST_TOPIC_NAME));
        LOGGER.info("topic deleted: {}", TEST_TOPIC_NAME);


        assertThrows(HTTPNotFoundException.class,
            () -> await(kafkaAdminAPI.getTopic(TEST_TOPIC_NAME)),
            "get test-topic should fail due to topic being deleted in current test");
        LOGGER.info("topic not found : {}", TEST_TOPIC_NAME);
    }

    @Test
    @Order(5)
    void testDeleteNotExistingTopic() {
        assertKafkaAdminAPI();

        assertThrows(HTTPNotFoundException.class,
            () -> await(kafkaAdminAPI.deleteTopic(TEST_NOT_EXISTING_TOPIC_NAME)),
            "deleting not existing topic should fail");
        LOGGER.info("topic not found : {}", TEST_NOT_EXISTING_TOPIC_NAME);
    }

    @Test
    @Order(3)
    void testStartConsumerGroup() throws Throwable {
        assertKafkaAdminAPI();

        LOGGER.info("create or retrieve service account: {}", SERVICE_ACCOUNT_NAME);
        var account = await(ServiceAPIUtils.applyServiceAccount(serviceAPI, SERVICE_ACCOUNT_NAME));

        LOGGER.info("crete kafka consumer with group id: {}", TEST_GROUP_NAME);
        var consumer = KafkaConsumerClient.createConsumer(vertx,
            kafka.bootstrapServerHost,
            account.clientID,
            account.clientSecret,
            TEST_GROUP_NAME);

        LOGGER.info("subscribe to topic: {}", TEST_TOPIC_NAME);
        consumer.subscribe(TEST_TOPIC_NAME);
        consumer.handler(r -> {
            // ignore
        });

        LOGGER.info("wait for consumer group to ");

        IsReady<Object> subscribed = last -> consumer.assignment().map(partitions -> {
            var o = partitions.stream().filter(p -> p.getTopic().equals(TEST_TOPIC_NAME)).findAny();
            return Pair.with(o.isPresent(), null);
        });
        await(waitFor(vertx, "consumer group to subscribe", Duration.ofSeconds(2), Duration.ofMinutes(2), subscribed));

        kafkaConsumer = consumer;
    }

    @Test
    @Order(4)
    void testGetAllConsumerGroups() throws Throwable {
        assertKafkaAdminAPI();
        assertConsumerGroup();

        var groups = await(kafkaAdminAPI.getAllConsumerGroups());
        LOGGER.info("got consumer groups: {}", Json.encode(groups));

        assertTrue(groups.items.size() >= 1);
    }

    @Test
    @Order(4)
    void testGetConsumerGroup() throws Throwable {
        assertKafkaAdminAPI();
        assertConsumerGroup();

        var group = await(kafkaAdminAPI.getConsumerGroup(TEST_GROUP_NAME));
        LOGGER.info("consumer group: {}", Json.encode(group));

        assertEquals(group.groupId, TEST_GROUP_NAME);
        assertTrue(group.consumers.size() > 0);
    }

    @Test
    @Order(4)
    void testGetNotExistingConsumerGroup() {
        assertKafkaAdminAPI();

        assertThrows(HTTPNotFoundException.class,
            () -> await(kafkaAdminAPI.getConsumerGroup(TEST_NOT_EXISTING_GROUP_NAME)),
            message("get consumer group '{}' should fail", TEST_NOT_EXISTING_GROUP_NAME));
        LOGGER.info("consumer group '{}' doesn't exists", TEST_NOT_EXISTING_GROUP_NAME);
    }

    @Test
    @Order(4)
    void testDeleteActiveConsumerGroup() {
        assertKafkaAdminAPI();
        assertConsumerGroup();

        assertThrows(HTTPLockedException.class,
            () -> await(kafkaAdminAPI.deleteConsumerGroup(TEST_GROUP_NAME)),
            "deleting active consumer group should fail");
        LOGGER.info("active consumer group cannot be deleted: {}", TEST_GROUP_NAME);
    }

    @Test
    @Order(4)
    void testDeleteNotExistingConsumerGroup() {
        assertKafkaAdminAPI();

        assertThrows(HTTPNotFoundException.class,
            () -> await(kafkaAdminAPI.deleteConsumerGroup(TEST_NOT_EXISTING_GROUP_NAME)),
            "deleting not existing consumer group should fail");
        LOGGER.info("not existing consumer group cannot be deleted: {}", TEST_NOT_EXISTING_TOPIC_NAME);
    }

    @Test
    @Order(5)
    void testDeleteConsumerGroup() throws Throwable {
        assertKafkaAdminAPI();
        assertConsumerGroup();

        LOGGER.info("close kafka consumer");
        await(kafkaConsumer.close());

        LOGGER.info("delete consumer group: {}", TEST_GROUP_NAME);
        await(kafkaAdminAPI.deleteConsumerGroup(TEST_GROUP_NAME));

        assertThrows(HTTPNotFoundException.class,
            () -> await(kafkaAdminAPI.getConsumerGroup(TEST_GROUP_NAME)),
            message("consumer group '{}' should had been deleted", TEST_GROUP_NAME));
    }
}