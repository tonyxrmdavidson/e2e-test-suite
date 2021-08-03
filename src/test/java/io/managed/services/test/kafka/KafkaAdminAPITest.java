package io.managed.services.test.kafka;

import io.managed.services.test.Environment;
import io.managed.services.test.IsReady;
import io.managed.services.test.TestBase;
import io.managed.services.test.TestUtils;
import io.managed.services.test.client.exception.HTTPConflictException;
import io.managed.services.test.client.exception.HTTPLockedException;
import io.managed.services.test.client.exception.HTTPNotFoundException;
import io.managed.services.test.client.exception.HTTPUnauthorizedException;
import io.managed.services.test.client.kafka.KafkaAuthMethod;
import io.managed.services.test.client.kafka.KafkaConsumerClient;
import io.managed.services.test.client.kafkaadminapi.ConsumerGroup;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.waitFor;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

/**
 * Test the main endpoints of the kafka-admin-api[1] that is deployed alongside each Kafka Instance
 * and used to administer the Kafka Instance itself.
 * <p>
 * 1. https://github.com/bf2fc6cc711aee1a0c2a/kafka-admin-api
 */
@Test(groups = TestTag.KAFKA_ADMIN_API)
public class KafkaAdminAPITest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(KafkaAdminAPITest.class);

    private static final String KAFKA_INSTANCE_NAME = "mk-e2e-kaa-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String SERVICE_ACCOUNT_NAME = "mk-e2e-kaa-sa-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String TEST_TOPIC_NAME = "test-api-topic-1";
    private static final String TEST_NOT_EXISTING_TOPIC_NAME = "test-api-topic-not-exist";

    private static final String TEST_GROUP_NAME = "test-consumer-group";
    private static final String TEST_NOT_EXISTING_GROUP_NAME = "not-existing-group";

    private final Vertx vertx = Vertx.vertx();

    private KafkaAdminAPI kafkaAdminAPI;
    private ServiceAPI serviceAPI;
    private KafkaResponse kafka;
    private KafkaConsumer<String, String> kafkaConsumer;

    // TODO: Test update topic with random values

    @BeforeClass(timeOut = 10 * MINUTES)
    public void bootstrap() throws Throwable {
        serviceAPI = bwait(ServiceAPIUtils.serviceAPI(vertx));
        LOGGER.info("service api initialized");

        kafka = bwait(ServiceAPIUtils.applyKafkaInstance(vertx, serviceAPI, KAFKA_INSTANCE_NAME));
        LOGGER.info("kafka instance created: {}", Json.encode(kafka));

        var bootstrapServerHost = kafka.bootstrapServerHost;
        kafkaAdminAPI = bwait(KafkaAdminAPIUtils.kafkaAdminAPI(vertx, bootstrapServerHost));
        LOGGER.info("kafka admin api client initialized");
    }

    @AfterClass(timeOut = DEFAULT_TIMEOUT, alwaysRun = true)
    public void teardown() throws Throwable {
        assumeTeardown();

        // delete kafka instance
        try {
            bwait(ServiceAPIUtils.cleanKafkaInstance(serviceAPI, KAFKA_INSTANCE_NAME));
        } catch (Throwable t) {
            LOGGER.error("failed to clean kafka instance: ", t);
        }

        // delete service account
        try {
            bwait(ServiceAPIUtils.deleteServiceAccountByNameIfExists(serviceAPI, SERVICE_ACCOUNT_NAME));
        } catch (Throwable t) {
            LOGGER.error("failed to clean service account: ", t);
        }

        // close vertx
        bwait(vertx.close());
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testFailToCallAPIIfUserBelongsToADifferentOrganization() throws Throwable {

        LOGGER.info("Test different organisation user");
        var bootstrapServerHost = kafka.bootstrapServerHost;
        var kafkaAdminAPIDifferentOrganization = bwait(KafkaAdminAPIUtils.kafkaAdminAPI(
            vertx,
            bootstrapServerHost,
            Environment.SSO_ALIEN_USERNAME,
            Environment.SSO_ALIEN_PASSWORD
        ));
        assertThrows(HTTPUnauthorizedException.class, () ->
            bwait(kafkaAdminAPIDifferentOrganization.getAllTopics()));
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testFailToCallAPIIfUserDoesNotOwnTheKafkaInstance() throws Throwable {

        LOGGER.info("Test same organisation user");
        var bootstrapServerHost = kafka.bootstrapServerHost;
        var kafkaAdminAPISameOrganisationUser = bwait(KafkaAdminAPIUtils.kafkaAdminAPI(
            vertx,
            bootstrapServerHost,
            Environment.SSO_SECONDARY_USERNAME,
            Environment.SSO_SECONDARY_PASSWORD
        ));
        assertThrows(HTTPUnauthorizedException.class, () ->
            bwait(kafkaAdminAPISameOrganisationUser.getAllTopics()));
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testFailToCallAPIIfTokenIsInvalid() {

        LOGGER.info("Test invalid token");
        var bootstrapServerHost = kafka.bootstrapServerHost;
        var apiURI = String.format("%s%s", Environment.KAFKA_ADMIN_API_SERVER_PREFIX, bootstrapServerHost);
        KafkaAdminAPI kafkaAdminAPIUnauthorizedUser = new KafkaAdminAPI(vertx, apiURI, TestUtils.FAKE_TOKEN);
        assertThrows(HTTPUnauthorizedException.class, () ->
            bwait(kafkaAdminAPIUnauthorizedUser.getAllTopics()));
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testCreateTopic() throws Throwable {
        // getting test-topic should fail because the topic shouldn't exists
        assertThrows(HTTPNotFoundException.class,
            () -> bwait(kafkaAdminAPI.getTopic(TEST_TOPIC_NAME)));
        LOGGER.info("topic not found : {}", TEST_TOPIC_NAME);

        LOGGER.info("create topic: {}", TEST_TOPIC_NAME);
        // TODO: Randomize topic configuration where possible
        bwait(KafkaAdminAPIUtils.createDefaultTopic(kafkaAdminAPI, TEST_TOPIC_NAME));

        LOGGER.info("topic created: {}", TEST_TOPIC_NAME);

        // TODO: Test the topic
    }

    @Test(dependsOnMethods = "testCreateTopic", timeOut = DEFAULT_TIMEOUT)
    public void testFailToCreateTopicIfItAlreadyExist() {
        // create existing topic should fail
        assertThrows(HTTPConflictException.class,
            () -> bwait(KafkaAdminAPIUtils.createDefaultTopic(kafkaAdminAPI, TEST_TOPIC_NAME)));

        LOGGER.info("existing topic cannot be created again : {}", TEST_TOPIC_NAME);
    }

    @Test(dependsOnMethods = "testCreateTopic", timeOut = DEFAULT_TIMEOUT)
    public void testGetTopicByName() throws Throwable {
        var t = bwait(kafkaAdminAPI.getTopic(TEST_TOPIC_NAME));
        LOGGER.info("topic retrieved: {}", Json.encode(t));

        assertEquals(TEST_TOPIC_NAME, t.name);
    }

    @Test(dependsOnMethods = "testCreateTopic", timeOut = DEFAULT_TIMEOUT)
    public void testFailToGetTopicIfItDoesNotExist() {
        // get none existing topic should fail
        assertThrows(HTTPNotFoundException.class,
            () -> bwait(kafkaAdminAPI.getTopic(TEST_NOT_EXISTING_TOPIC_NAME)));

        LOGGER.info("topic not found : {}", TEST_NOT_EXISTING_TOPIC_NAME);
    }

    @Test(dependsOnMethods = "testCreateTopic", timeOut = DEFAULT_TIMEOUT)
    public void tetGetAllTopics() throws Throwable {
        var topics = bwait(kafkaAdminAPI.getAllTopics());
        LOGGER.info("topics: {}", Json.encode(topics));
        List<Topic> filteredTopics = topics.items.stream()
            .filter(k -> k.name.equals(TEST_TOPIC_NAME))
            .collect(Collectors.toList());

        assertEquals(1, filteredTopics.size());
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testFailToDeleteTopicIfItDoesNotExist() {
        // deleting not existing topic should fail
        assertThrows(HTTPNotFoundException.class,
            () -> bwait(kafkaAdminAPI.deleteTopic(TEST_NOT_EXISTING_TOPIC_NAME)));
        LOGGER.info("topic not found : {}", TEST_NOT_EXISTING_TOPIC_NAME);
    }

    @Test(dependsOnMethods = "testCreateTopic", timeOut = DEFAULT_TIMEOUT)
    public void startConsumerGroup() throws Throwable {
        LOGGER.info("create or retrieve service account: {}", SERVICE_ACCOUNT_NAME);
        var account = bwait(ServiceAPIUtils.applyServiceAccount(serviceAPI, SERVICE_ACCOUNT_NAME));

        LOGGER.info("crete kafka consumer with group id: {}", TEST_GROUP_NAME);
        var consumer = KafkaConsumerClient.createConsumer(vertx,
            kafka.bootstrapServerHost,
            account.clientID,
            account.clientSecret,
            KafkaAuthMethod.OAUTH,
            TEST_GROUP_NAME,
            "latest");

        LOGGER.info("subscribe to topic: {}", TEST_TOPIC_NAME);
        consumer.subscribe(TEST_TOPIC_NAME);
        consumer.handler(r -> {
            // ignore
        });

        IsReady<Object> subscribed = last -> consumer.assignment().map(partitions -> {
            var o = partitions.stream().filter(p -> p.getTopic().equals(TEST_TOPIC_NAME)).findAny();
            return Pair.with(o.isPresent(), null);
        });
        bwait(waitFor(vertx, "consumer group to subscribe", ofSeconds(2), ofMinutes(2), subscribed));

        kafkaConsumer = consumer;
    }


    @Test(dependsOnMethods = "startConsumerGroup", timeOut = DEFAULT_TIMEOUT)
    public void testGetAllConsumerGroups() throws Throwable {
        var groups = bwait(kafkaAdminAPI.getAllConsumerGroups());
        LOGGER.info("got consumer groups: {}", Json.encode(groups));

        assertTrue(groups.items.size() >= 1);
    }


    @Test(dependsOnMethods = "startConsumerGroup", timeOut = DEFAULT_TIMEOUT)
    public void testGetConsumerGroup() throws Throwable {
        IsReady<ConsumerGroup> ready = last -> kafkaAdminAPI.getConsumerGroup(TEST_GROUP_NAME).map(consumerGroup -> {
            if (last) {
                LOGGER.warn("last consumer group: {}", Json.encode(consumerGroup));
            }

            // wait for the consumer group to show at least one consumer
            // because it could take a few seconds for the kafka admin api to
            // report the connected consumer
            return Pair.with(consumerGroup.consumers.size() > 0, consumerGroup);
        });
        var group = bwait(waitFor(vertx, "consumers in consumer group", ofSeconds(2), ofMinutes(1), ready));
        LOGGER.info("consumer group: {}", Json.encode(group));

        assertEquals(group.groupId, TEST_GROUP_NAME);
        assertTrue(group.consumers.size() > 0);
    }


    @Test(dependsOnMethods = "startConsumerGroup", timeOut = DEFAULT_TIMEOUT)
    public void testFailToGetConsumerGroupIfItDoesNotExist() {
        // get consumer group non existing consumer group should fail
        assertThrows(HTTPNotFoundException.class,
            () -> bwait(kafkaAdminAPI.getConsumerGroup(TEST_NOT_EXISTING_GROUP_NAME)));
        LOGGER.info("consumer group '{}' doesn't exists", TEST_NOT_EXISTING_GROUP_NAME);
    }

    @Test(dependsOnMethods = "startConsumerGroup", timeOut = DEFAULT_TIMEOUT)
    public void testFailToDeleteConsumerGroupIfItIsActive() {
        // deleting active consumer group should fail
        assertThrows(HTTPLockedException.class,
            () -> bwait(kafkaAdminAPI.deleteConsumerGroup(TEST_GROUP_NAME)));
        LOGGER.info("active consumer group cannot be deleted: {}", TEST_GROUP_NAME);
    }

    @Test(dependsOnMethods = "startConsumerGroup", timeOut = DEFAULT_TIMEOUT)
    public void testFailToDeleteConsumerGroupIfItDoesNotExist() {
        // deleting not existing consumer group should fail
        assertThrows(HTTPNotFoundException.class,
            () -> bwait(kafkaAdminAPI.deleteConsumerGroup(TEST_NOT_EXISTING_GROUP_NAME)));
        LOGGER.info("not existing consumer group cannot be deleted: {}", TEST_NOT_EXISTING_TOPIC_NAME);
    }

    @Test(dependsOnMethods = "startConsumerGroup", priority = 1, timeOut = DEFAULT_TIMEOUT)
    public void testDeleteConsumerGroup() throws Throwable {
        LOGGER.info("close kafka consumer");
        bwait(kafkaConsumer.close());

        LOGGER.info("delete consumer group: {}", TEST_GROUP_NAME);
        bwait(kafkaAdminAPI.deleteConsumerGroup(TEST_GROUP_NAME));

        // consumer group should had been deleted
        assertThrows(HTTPNotFoundException.class,
            () -> bwait(kafkaAdminAPI.getConsumerGroup(TEST_GROUP_NAME)));
        LOGGER.info("consumer group not found : {}", TEST_GROUP_NAME);
    }

    @Test(dependsOnMethods = "testCreateTopic", priority = 2, timeOut = DEFAULT_TIMEOUT)
    public void testDeleteTopic() throws Throwable {
        bwait(kafkaAdminAPI.deleteTopic(TEST_TOPIC_NAME));
        LOGGER.info("topic deleted: {}", TEST_TOPIC_NAME);

        // get test-topic should fail due to topic being deleted in current test
        assertThrows(HTTPNotFoundException.class,
            () -> bwait(kafkaAdminAPI.getTopic(TEST_TOPIC_NAME)));
        LOGGER.info("topic not found : {}", TEST_TOPIC_NAME);
    }
}