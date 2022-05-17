package io.managed.services.test.kafka;

import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.TopicSettings;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.TestUtils;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.exception.ApiConflictException;
import io.managed.services.test.client.exception.ApiLockedException;
import io.managed.services.test.client.exception.ApiNotFoundException;
import io.managed.services.test.client.exception.ApiUnauthorizedException;
import io.managed.services.test.client.kafka.KafkaConsumerClient;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiAccessUtils;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.oauth.KeycloakLoginSession;
import io.managed.services.test.client.oauth.KeycloakUser;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Objects;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

/**
 * Test the main endpoints of the kafka-admin-api[1] that is deployed alongside each Kafka Instance
 * and used to administer the Kafka Instance itself.
 * <p>
 * 1. https://github.com/bf2fc6cc711aee1a0c2a/kafka-admin-api
 * <p>
 * <b>Requires:</b>
 * <ul>
 *     <li> PRIMARY_USERNAME
 *     <li> PRIMARY_PASSWORD
 * </ul>
 */
public class KafkaInstanceAPITest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(KafkaInstanceAPITest.class);

    private static final String KAFKA_INSTANCE_NAME = "mk-e2e-kaa-" + Environment.LAUNCH_KEY;
    private static final String SERVICE_ACCOUNT_NAME = "mk-e2e-kaa-sa-" + Environment.LAUNCH_KEY;
    private static final String TEST_TOPIC_NAME = "test-api-topic-1";
    private static final String TEST_NOT_EXISTING_TOPIC_NAME = "test-api-topic-not-exist";

    private static final String TEST_GROUP_NAME = "test-consumer-group";
    private static final String TEST_NOT_EXISTING_GROUP_NAME = "not-existing-group";

    private final Vertx vertx = Vertx.vertx();

    private KafkaInstanceApi kafkaInstanceApi;
    private KafkaMgmtApi kafkaMgmtApi;
    private SecurityMgmtApi securityMgmtApi;
    private KafkaRequest kafka;
    private KafkaConsumerClient<String, String> kafkaConsumer;

    // TODO: Test update topic with random values

    @BeforeClass
    @SneakyThrows
    public void bootstrap() {
        assertNotNull(Environment.PRIMARY_USERNAME, "the PRIMARY_USERNAME env is null");
        assertNotNull(Environment.PRIMARY_PASSWORD, "the PRIMARY_PASSWORD env is null");

        var auth = new KeycloakLoginSession(Environment.PRIMARY_USERNAME, Environment.PRIMARY_PASSWORD);
        var apps = ApplicationServicesApi.applicationServicesApi(auth);
        kafkaMgmtApi = apps.kafkaMgmt();
        securityMgmtApi = apps.securityMgmt();
        LOGGER.info("kafka and security mgmt api initialized");

        kafka = KafkaMgmtApiUtils.applyKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);

        kafkaInstanceApi = bwait(KafkaInstanceApiUtils.kafkaInstanceApi(auth, kafka));
        LOGGER.info("kafka instance api client initialized");
    }

    @AfterClass(alwaysRun = true)
    public void teardown() {
        assumeTeardown();

        // delete kafka instance
        try {
            KafkaMgmtApiUtils.cleanKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            LOGGER.error("failed to clean kafka instance: ", t);
        }

        // delete service account
        try {
            SecurityMgmtAPIUtils.cleanServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            LOGGER.error("failed to clean service account: ", t);
        }

        try {
            if (kafkaConsumer != null) {
                bwait(kafkaConsumer.asyncClose());
            }
        } catch (Throwable t) {
            LOGGER.error("failed to close consumer: ", t);
        }

        try {
            bwait(vertx.close());
        } catch (Throwable t) {
            LOGGER.error("failed to close vertx: ", t);
        }
    }

    @Test
    @SneakyThrows
    public void testFailToCallAPIIfUserBelongsToADifferentOrganization() {

        var kafkaInstanceApi = bwait(KafkaInstanceApiUtils.kafkaInstanceApi(
            new KeycloakLoginSession(Environment.ALIEN_USERNAME, Environment.ALIEN_PASSWORD), kafka));
        assertThrows(ApiUnauthorizedException.class, () -> kafkaInstanceApi.getTopics());
    }

    @Test
    @SneakyThrows
    public void testFailToCallAPIIfTokenIsInvalid() {
        var kafkaInstanceApi = KafkaInstanceApiUtils.kafkaInstanceApi(
            KafkaInstanceApiUtils.kafkaInstanceApiUri(kafka), new KeycloakUser(TestUtils.FAKE_TOKEN));
        assertThrows(ApiUnauthorizedException.class, () -> kafkaInstanceApi.getTopics());
    }

    @Test
    @SneakyThrows
    public void testCreateTopic() {

        // getting test-topic should fail because the topic shouldn't exist
        assertThrows(ApiNotFoundException.class, () -> kafkaInstanceApi.getTopic(TEST_TOPIC_NAME));
        LOGGER.info("topic '{}' not found", TEST_TOPIC_NAME);

        LOGGER.info("create topic '{}'", TEST_TOPIC_NAME);
        var payload = new NewTopicInput()
            .name(TEST_TOPIC_NAME)
            .settings(new TopicSettings().numPartitions(1));
        var topic = kafkaInstanceApi.createTopic(payload);
        LOGGER.debug(topic);
    }

    @Test(dependsOnMethods = "testCreateTopic")
    public void testFailToCreateTopicIfItAlreadyExist() {
        // create existing topic should fail
        var payload = new NewTopicInput()
            .name(TEST_TOPIC_NAME)
            .settings(new TopicSettings().numPartitions(1));
        assertThrows(ApiConflictException.class,
            () -> kafkaInstanceApi.createTopic(payload));
    }

    @Test(dependsOnMethods = "testCreateTopic")
    @SneakyThrows
    public void testGetTopicByName() {
        var topic = kafkaInstanceApi.getTopic(TEST_TOPIC_NAME);
        LOGGER.debug(topic);
        assertEquals(topic.getName(), TEST_TOPIC_NAME);
    }

    @Test(dependsOnMethods = "testCreateTopic")
    public void testFailToGetTopicIfItDoesNotExist() {
        // get none existing topic should fail
        assertThrows(ApiNotFoundException.class,
            () -> kafkaInstanceApi.getTopic(TEST_NOT_EXISTING_TOPIC_NAME));
    }

    @Test(dependsOnMethods = "testCreateTopic")
    @SneakyThrows
    public void tetGetAllTopics() {
        var topics = kafkaInstanceApi.getTopics();
        LOGGER.debug(topics);

        var filteredTopics = Objects.requireNonNull(topics.getItems())
            .stream()
            .filter(k -> TEST_TOPIC_NAME.equals(k.getName()))
            .findAny();

        assertTrue(filteredTopics.isPresent());
    }

    @Test
    public void testFailToDeleteTopicIfItDoesNotExist() {
        // deleting not existing topic should fail
        assertThrows(ApiNotFoundException.class,
            () -> kafkaInstanceApi.deleteTopic(TEST_NOT_EXISTING_TOPIC_NAME));
    }

    @Test(dependsOnMethods = "testCreateTopic")
    @SneakyThrows
    public void testConsumerGroup() {
        LOGGER.info("create or retrieve service account '{}'", SERVICE_ACCOUNT_NAME);
        var account = SecurityMgmtAPIUtils.applyServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME);

        LOGGER.info("grant access to the service account '{}'", SERVICE_ACCOUNT_NAME);
        KafkaInstanceApiAccessUtils.createProducerAndConsumerACLs(kafkaInstanceApi, KafkaInstanceApiAccessUtils.toPrincipal(account.getClientId()));

        kafkaConsumer = bwait(KafkaInstanceApiUtils.startConsumerGroup(vertx,
            TEST_GROUP_NAME,
            TEST_TOPIC_NAME,
            kafka.getBootstrapServerHost(),
            account.getClientId(),
            account.getClientSecret()));

        var group = KafkaInstanceApiUtils.waitForConsumerGroup(kafkaInstanceApi, TEST_GROUP_NAME);
        LOGGER.debug(group);

        group = KafkaInstanceApiUtils.waitForConsumersInConsumerGroup(kafkaInstanceApi, group.getGroupId());
        LOGGER.debug(group);

        assertEquals(group.getGroupId(), TEST_GROUP_NAME);
        assertTrue(group.getConsumers().size() > 0);
    }

    @Test(dependsOnMethods = "testConsumerGroup")
    @SneakyThrows
    public void testGetAllConsumerGroups() {
        var groups = kafkaInstanceApi.getConsumerGroups();
        LOGGER.debug(groups);

        var filteredGroup = Objects.requireNonNull(groups.getItems())
            .stream()
            .filter(g -> TEST_GROUP_NAME.equals(g.getGroupId()))
            .findAny();

        assertTrue(filteredGroup.isPresent());
    }

    @Test(dependsOnMethods = "testConsumerGroup")
    public void testFailToGetConsumerGroupIfItDoesNotExist() {
        // get consumer group non-existing consumer group should fail
        assertThrows(ApiNotFoundException.class,
            () -> kafkaInstanceApi.getConsumerGroupById(TEST_NOT_EXISTING_GROUP_NAME));
    }

    @Test(dependsOnMethods = "testConsumerGroup")
    public void testFailToDeleteConsumerGroupIfItIsActive() {
        // deleting active consumer group should fail
        assertThrows(ApiLockedException.class,
            () -> kafkaInstanceApi.deleteConsumerGroupById(TEST_GROUP_NAME));
    }

    @Test(dependsOnMethods = "testConsumerGroup")
    public void testFailToDeleteConsumerGroupIfItDoesNotExist() {
        // deleting not existing consumer group should fail
        assertThrows(ApiNotFoundException.class,
            () -> kafkaInstanceApi.deleteConsumerGroupById(TEST_NOT_EXISTING_GROUP_NAME));
    }

    @Test(dependsOnMethods = "testConsumerGroup", priority = 1)
    public void testDeleteConsumerGroup() throws Throwable {
        LOGGER.info("close kafka consumer");
        bwait(kafkaConsumer.asyncClose());

        LOGGER.info("delete consumer group '{}'", TEST_GROUP_NAME);
        kafkaInstanceApi.deleteConsumerGroupById(TEST_GROUP_NAME);

        // consumer group should have been deleted
        assertThrows(ApiNotFoundException.class,
            () -> kafkaInstanceApi.getConsumerGroupById(TEST_GROUP_NAME));
        LOGGER.info("consumer group '{}' not found", TEST_GROUP_NAME);
    }

    @Test(dependsOnMethods = "testCreateTopic", priority = 2)
    public void testDeleteTopic() throws Throwable {
        kafkaInstanceApi.deleteTopic(TEST_TOPIC_NAME);
        LOGGER.info("topic '{}' deleted", TEST_TOPIC_NAME);

        // get test-topic should fail due to topic being deleted in current test
        assertThrows(ApiNotFoundException.class,
            () -> kafkaInstanceApi.getTopic(TEST_TOPIC_NAME));
        LOGGER.info("topic '{}' not found", TEST_TOPIC_NAME);
    }
}
