package io.managed.services.test.kafkainstances;

import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.kafka.KafkaAuthMethod;
import io.managed.services.test.client.kafka.KafkaConsumerClient;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.DelegationTokenDisabledException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.sleep;
import static java.time.Duration.ofSeconds;
import static org.testng.Assert.assertThrows;

/**
 * Test the configured ACLs for a Kafka Instance using the Kafka Admin[1] library which is the same Java library used
 * by the kafka bin scripts.
 * <p>
 * 1. https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/admin/Admin.java
 */
@Test(groups = TestTag.KAFKA_ADMIN_PERMISSIONS)
public class KafkaAdminPermissionTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(KafkaAdminPermissionTest.class);

    private static final String KAFKA_INSTANCE_NAME = "mk-e2e-pe-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String SERVICE_ACCOUNT_NAME = "mk-e2e-pe-sa-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String TOPIC_NAME = "test-topic-1";
    private static final String STRIMZI_CANARY_TOPIC = "__strimzi_canary";
    private static final String STRIMZI_CANARY_GROUP = "strimzi-canary-group";
    private static final String TEST_GROUP_ID = "new-group-id";

    private final Vertx vertx = Vertx.vertx();

    private ServiceAPI serviceAPI;
    private KafkaAdmin admin;
    private KafkaConsumer<String, String> consumer;

    // TODO: Move out from the canary topic and consumer group

    @AfterClass(timeOut = DEFAULT_TIMEOUT, alwaysRun = true)
    public void teardown() throws Throwable {
        // close KafkaAdmin
        if (admin != null) admin.close();

        assumeTeardown();

        // close the consumer
        try {
            consumer.close();
        } catch (Throwable t) {
            LOGGER.error("failed to close the consumer: ", t);
        }

        // delete service account
        try {
            bwait(ServiceAPIUtils.deleteServiceAccountByNameIfExists(serviceAPI, SERVICE_ACCOUNT_NAME));
        } catch (Throwable t) {
            LOGGER.error("clean service account error: ", t);
        }

        // delete kafka instance
        try {
            bwait(ServiceAPIUtils.deleteKafkaByNameIfExists(serviceAPI, KAFKA_INSTANCE_NAME));
        } catch (Throwable t) {
            LOGGER.error("clean kafka error: ", t);
        }

        // close vertx
        bwait(vertx.close());
    }

    @BeforeClass(timeOut = 15 * MINUTES)
    public void bootstrap() throws Throwable {
        // create the serviceAPI
        serviceAPI = bwait(ServiceAPIUtils.serviceAPI(vertx));

        // create the kafka admin
        var kafka = bwait(ServiceAPIUtils.applyKafkaInstance(vertx, serviceAPI, KAFKA_INSTANCE_NAME));
        LOGGER.info("kafka instance connected/created: {}", kafka.name);

        var serviceAccount = bwait(ServiceAPIUtils.applyServiceAccount(serviceAPI, SERVICE_ACCOUNT_NAME));
        LOGGER.info("service account created/connected: {}", serviceAccount.name);

        var bootstrapHost = kafka.bootstrapServerHost;
        var clientID = serviceAccount.clientID;
        var clientSecret = serviceAccount.clientSecret;
        admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);
        LOGGER.info("kafka admin api initialized for instance: {}", bootstrapHost);

        // setup a consumer to create the group
        consumer = KafkaConsumerClient.createConsumer(
            vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            KafkaAuthMethod.PLAIN,
            TEST_GROUP_ID,
            "latest");
        bwait(consumer.subscribe(STRIMZI_CANARY_TOPIC));
        consumer.handler(r -> {
            // ignore
        });
        LOGGER.info("started a new consumer in the consumer group: {}", TEST_GROUP_ID);

        bwait(sleep(vertx, ofSeconds(3)));
        LOGGER.info("close the consumer to release the consumer consumer group");
        bwait(consumer.close());
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testAllowedToCreateTopic() throws Throwable {

        LOGGER.info("kafka-topics.sh --create <Permitted>, script representation test");
        bwait(admin.createTopic(TOPIC_NAME));

        LOGGER.info("topic successfully created: {}", TOPIC_NAME);
    }

    @Test(dependsOnMethods = "testAllowedToCreateTopic", timeOut = DEFAULT_TIMEOUT)
    public void testAllowedToListTopic() throws Throwable {

        LOGGER.info("kafka-topics.sh --list <Permitted>, script representation test");
        var r = bwait(admin.listTopics());

        LOGGER.info("topics successfully listed, response contains {} topic/s", r.size());
    }

    @Test(dependsOnMethods = "testAllowedToCreateTopic", priority = 1, timeOut = DEFAULT_TIMEOUT)
    public void testAllowedToDeleteTopic() throws Throwable {

        LOGGER.info("kafka-topics.sh --delete <Permitted>, script representation test");
        LOGGER.info("delete created topic : {}", TOPIC_NAME);
        bwait(admin.deleteTopic(TOPIC_NAME));

        LOGGER.info("topic {} successfully deleted", TOPIC_NAME);
    }

    @DataProvider
    public Object[][] aclAddCmdProvider() {
        return new Object[][] {
            {"--add--cluster", ResourceType.CLUSTER},
            {"--add--topic", ResourceType.TOPIC},
            {"--add--group", ResourceType.GROUP},
            {"--add--delegation-token", ResourceType.DELEGATION_TOKEN},
            {"--ad--transactional-id", ResourceType.TRANSACTIONAL_ID},
        };
    }

    @Test(dataProvider = "aclAddCmdProvider", timeOut = DEFAULT_TIMEOUT)
    public void testForbiddenToAlterACLResource(String testName, ResourceType resourceType) {
        LOGGER.info("kafka-acls.sh {} <Forbidden>, script representation test", testName);
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.addAclResource(resourceType)));
    }

    @DataProvider
    public Object[][] aclListCmdProvider() {
        return new Object[][] {
            {"--list--topic", ResourceType.TOPIC},
            {"--list--cluster", ResourceType.CLUSTER},
            {"--list--group", ResourceType.GROUP},
            {"--list", ResourceType.ANY},
        };
    }

    @Test(dataProvider = "aclListCmdProvider", timeOut = DEFAULT_TIMEOUT)
    public void testForbiddenToListACLResource(String testName, ResourceType resourceType) {
        LOGGER.info("kafka-acls.sh {} <Forbidden>, script representation test", testName);
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.listAclResource(resourceType)));
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testAllowedToDescribeTopicConfiguration() throws Throwable {

        LOGGER.info("kafka-configs.sh --describe --entity-type topics <permitted>, script representation test");
        LOGGER.info("getting entity description for topic with name: {}", STRIMZI_CANARY_TOPIC);
        var r = bwait(admin.getConfigurationTopic(STRIMZI_CANARY_TOPIC));
        LOGGER.info("response size: {}", r.size());
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    @Ignore
    public void testAllowedToDescribeUserConfiguration() throws Throwable {

        LOGGER.info("kafka-configs.sh --describe --entity-type brokerLogger <permitted>, script representation test");
        LOGGER.info("getting entity description for default user");
        var r = bwait(admin.getConfigurationUserAll());
        LOGGER.info("user response: {}", r);
    }

    @DataProvider
    public Object[][] configureBrokerCmdProvider() {
        return new Object[][] {
            {"configAddBroker", ConfigResource.Type.BROKER, AlterConfigOp.OpType.APPEND},
            {"configDeleteBroker", ConfigResource.Type.BROKER, AlterConfigOp.OpType.DELETE},
            {"configAddBrokerLogger", ConfigResource.Type.BROKER_LOGGER, AlterConfigOp.OpType.DELETE},
            {"configDeleteBrokerLogger", ConfigResource.Type.BROKER_LOGGER, AlterConfigOp.OpType.DELETE}
        };
    }

    @Test(dataProvider = "configureBrokerCmdProvider", timeOut = DEFAULT_TIMEOUT)
    public void testForbiddenToAlterBrokerConfig(String testName, ConfigResource.Type resourceType, AlterConfigOp.OpType opType) {
        LOGGER.info("kafka-config.sh {} <allowed>, script representation test", testName);
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.configureBrokerResource(resourceType, opType, "0")));
    }

    @Test(dependsOnMethods = "testAllowedToCreateTopic", timeOut = DEFAULT_TIMEOUT)
    public void testAllowedToDeleteTopicConfig() throws Throwable {

        LOGGER.info("kafka-config.sh --alter --entity-type topics --delete-config <allowed>, script representation test");
        var r = bwait(admin.configureBrokerResource(ConfigResource.Type.TOPIC, AlterConfigOp.OpType.DELETE, TOPIC_NAME));
        LOGGER.info("topic configured: {}", r);
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testForbiddenToDescribeBrokerConfig() {

        LOGGER.info("kafka-configs.sh --describe --entity-type broker <forbidden>, script representation test");
        LOGGER.info("getting entity description for broker: 0");
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.getConfigurationBroker("0")));
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testForbiddenToDescribeBrokerLoggerConfig() {

        LOGGER.info("kafka-configs.sh --describe --entity-type brokerLogger <forbidden>, script representation test");
        LOGGER.info("getting entity description for brokerLogger: 0");
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.getConfigurationBrokerLogger("0")));
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    @Ignore
    public void testForbiddenToAlterUserConfig() {

        LOGGER.info("kafka-configs.sh --alter --entity-type brokerLogger <permitted>, script representation test");
        // configuration-alter-users fail only due not previously existing configuration, but operation is allowed
        assertThrows(InvalidRequestException.class, () -> bwait(admin.alterConfigurationUser()));
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testAllowedToListConsumerGroups() throws Throwable {

        LOGGER.info("kafka-consumer-groups.sh --list <permitted>, script representation test");
        LOGGER.info("listing all consumer groups");
        var r = bwait(admin.listConsumerGroups());
        LOGGER.info("list consumer groups: {}", r);
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testAllowedToDescribeConsumerGroup() throws Throwable {

        LOGGER.info("kafka-consumer-groups.sh --group --describe <permitted>, script representation test");
        LOGGER.info("describing specific consumer group");
        var r = bwait(admin.describeConsumerGroups(STRIMZI_CANARY_GROUP));
        LOGGER.info("describe consumer groups: {}", r);
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testFailToDeleteActiveConsumerGroup() {

        LOGGER.info("kafka-consumer-groups.sh --all-groups --delete  <permitted>, script representation test");
        LOGGER.info("deleting group");
        // should fail because the canary consumer group is not empty
        assertThrows(GroupNotEmptyException.class, () -> bwait(admin.deleteConsumerGroups(STRIMZI_CANARY_GROUP)));
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testAllowedToResetConsumerGroupOffset() throws Throwable {

        LOGGER.info("kafka-consumer-groups.sh --all-groups --reset-offsets --execute --all-groups --all-topics  <permitted>, script representation test");
        bwait(admin.resetOffsets(STRIMZI_CANARY_TOPIC, TEST_GROUP_ID));
        LOGGER.info("offset successfully reset");
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testAllowedToDeleteConsumerGroupOffset() throws Throwable {

        LOGGER.info("kafka-consumer-groups.sh --delete-offsets  <permitted>, script representation test");
        bwait(admin.deleteOffset(STRIMZI_CANARY_TOPIC, TEST_GROUP_ID));
        LOGGER.info("offset successfully deleted");
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testAllowedToDeleteRecords() throws Throwable {

        LOGGER.info("kafka-delete-records.sh --offset-json-file <permitted>, script representation test");
        bwait(admin.deleteRecords(STRIMZI_CANARY_TOPIC));
        LOGGER.info("record successfully deleted");
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testForbiddenToUncleanLeaderElection() {

        LOGGER.info("kafka-leader-election.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.electLeader(ElectionType.UNCLEAN, STRIMZI_CANARY_TOPIC)));
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testForbiddenToDescribeLogDirs() {

        LOGGER.info("kafka-log-dirs.sh --describe <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.logDirs()));
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testForbiddenToAlterPreferredReplicaElection() {

        LOGGER.info("kafka-preferred-replica-election.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.electLeader(ElectionType.PREFERRED, STRIMZI_CANARY_TOPIC)));
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testForbiddenToReassignPartitions() {

        LOGGER.info("kafka-reassign-partitions.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.reassignPartitions(STRIMZI_CANARY_TOPIC)));
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testForbiddenToCreateDelegationToken() {

        LOGGER.info("kafka-delegation-tokens.sh create <forbidden>, script representation test");
        assertThrows(DelegationTokenDisabledException.class, () -> bwait(admin.createDelegationToken()));
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testForbiddenToDescribeDelegationToken() {

        LOGGER.info("kafka-delegation-tokens.sh describe <forbidden>, script representation test");
        assertThrows(DelegationTokenDisabledException.class, () -> bwait(admin.describeDelegationToken()));
    }
}