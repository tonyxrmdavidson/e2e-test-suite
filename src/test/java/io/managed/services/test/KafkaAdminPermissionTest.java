package io.managed.services.test;

import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
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
import static org.testng.Assert.assertThrows;

@Test(groups = TestTag.KAFKA_ADMIN_PERMISSIONS)
public class KafkaAdminPermissionTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(KafkaAdminPermissionTest.class);

    private static final String KAFKA_INSTANCE_NAME = "mk-e2e-pe-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String SERVICE_ACCOUNT_NAME = "mk-e2e-pe-sa-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String TOPIC_NAME = "test-topic-1";
    private static final String PERSISTENT_TOPIC = "__strimzi_canary";

    private final Vertx vertx = Vertx.vertx();

    private ServiceAPI serviceAPI;
    private KafkaAdmin admin;

    @AfterClass(timeOut = DEFAULT_TIMEOUT)
    public void teardown() {
        // close KafkaAdmin
        if (admin != null) admin.close();

        assumeTeardown();

        // delete service account
        try {
            bwait(ServiceAPIUtils.deleteServiceAccountByNameIfExists(serviceAPI, SERVICE_ACCOUNT_NAME));
        } catch (Throwable t) {
            LOGGER.error("clean service account error: ", t);
        }

        // delete kafka instance
        try {
            bwait(ServiceAPIUtils.deleteKafkaByNameIfExists(serviceAPI, SERVICE_ACCOUNT_NAME));
        } catch (Throwable t) {
            LOGGER.error("clean kafka error: ", t);
        }
    }

    @BeforeClass(timeOut = DEFAULT_TIMEOUT)
    public void bootstrap() throws Throwable {
        serviceAPI = bwait(ServiceAPIUtils.serviceAPI(vertx));
    }

    @Test(timeOut = 15 * MINUTES)
    public void testBootstrapKafkaAdmin() throws Throwable {

        var kafka = bwait(ServiceAPIUtils.applyKafkaInstance(vertx, serviceAPI, KAFKA_INSTANCE_NAME));
        LOGGER.info("kafka instance connected/created: {}", kafka.name);

        var serviceAccount = bwait(ServiceAPIUtils.applyServiceAccount(serviceAPI, SERVICE_ACCOUNT_NAME));
        LOGGER.info("service account created/connected: {}", serviceAccount.name);

        var bootstrapHost = kafka.bootstrapServerHost;
        var clientID = serviceAccount.clientID;
        var clientSecret = serviceAccount.clientSecret;
        admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);
    }

    @Test(dependsOnMethods = "testBootstrapKafkaAdmin", timeOut = DEFAULT_TIMEOUT)
    public void testTopicCreate() throws Throwable {

        LOGGER.info("kafka-topics.sh --create <Permitted>, script representation test");
        bwait(admin.createTopic(TOPIC_NAME));

        LOGGER.info("topic successfully created: {}", TOPIC_NAME);
    }

    @Test(dependsOnMethods = "testTopicCreate", timeOut = DEFAULT_TIMEOUT)
    public void testTopicList() throws Throwable {

        LOGGER.info("kafka-topics.sh --list <Permitted>, script representation test");
        var r = bwait(admin.listTopics());

        LOGGER.info("topics successfully listed, response contains {} topic/s", r.size());
    }

    @Test(dependsOnMethods = "testTopicCreate", priority = 1, timeOut = DEFAULT_TIMEOUT)
    public void testTopicDelete() throws Throwable {

        LOGGER.info("kafka-topics.sh --delete <Permitted>, script representation test");
        LOGGER.info("delete created topic : {}", TOPIC_NAME);
        bwait(admin.deleteTopic(TOPIC_NAME));

        LOGGER.info("topic {} successfully deleted", TOPIC_NAME);
    }

    @DataProvider
    public Object[][] aclProvider() {
        return new Object[][] {
            {"--add--cluster", ResourceType.CLUSTER},
            {"--add--topic", ResourceType.TOPIC},
            {"--add--group", ResourceType.GROUP},
            {"--add--delegation-token", ResourceType.DELEGATION_TOKEN},
            {"--ad--transactional-id", ResourceType.TRANSACTIONAL_ID},
        };
    }

    @DataProvider
    public Object[][] aclListProvider() {
        return new Object[][] {
            {"--list--topic", ResourceType.TOPIC},
            {"--list--cluster", ResourceType.CLUSTER},
            {"--list--group", ResourceType.GROUP},
            {"--list", ResourceType.ANY},
        };
    }

    @Test(dataProvider = "aclProvider", dependsOnMethods = "testBootstrapKafkaAdmin", timeOut = DEFAULT_TIMEOUT)
    public void testACLResource(String testName, ResourceType resourceType) {
        LOGGER.info("kafka-acls.sh {} <Forbidden>, script representation test", testName);
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.addAclResource(resourceType)));
    }

    @Test(dataProvider = "aclListProvider", dependsOnMethods = "testBootstrapKafkaAdmin", timeOut = DEFAULT_TIMEOUT)
    public void testACLListResource(String testName, ResourceType resourceType) {
        LOGGER.info("kafka-acls.sh {} <Forbidden>, script representation test", testName);
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.listAclResource(resourceType)));
    }

    @Test(dependsOnMethods = "testBootstrapKafkaAdmin", timeOut = DEFAULT_TIMEOUT)
    public void testGetConfigurationTopic() throws Throwable {

        LOGGER.info("kafka-configs.sh --describe --entity-type topics <permitted>, script representation test");
        LOGGER.info("getting entity description for topic with name: {}", PERSISTENT_TOPIC);
        var r = bwait(admin.getConfigurationTopic(PERSISTENT_TOPIC));
        LOGGER.info("response size: {}", r.size());
    }

    @Test(dependsOnMethods = "testBootstrapKafkaAdmin", timeOut = DEFAULT_TIMEOUT)
    @Ignore
    public void testGetConfigurationUsers() throws Throwable {

        LOGGER.info("kafka-configs.sh --describe --entity-type brokerLogger <permitted>, script representation test");
        LOGGER.info("getting entity description for default user");
        var r = bwait(admin.getConfigurationUserAll());
        LOGGER.info("user response: {}", r);
    }

    @DataProvider
    public Object[][] configureBrokerProvider() {
        return new Object[][] {
            {"configAddBroker", ConfigResource.Type.BROKER, AlterConfigOp.OpType.APPEND},
            {"configDeleteBroker", ConfigResource.Type.BROKER, AlterConfigOp.OpType.DELETE},
            {"configAddBrokerLogger", ConfigResource.Type.BROKER_LOGGER, AlterConfigOp.OpType.DELETE},
            {"configDeleteBrokerLogger", ConfigResource.Type.BROKER_LOGGER, AlterConfigOp.OpType.DELETE}
        };
    }

    @Test(dataProvider = "configureBrokerProvider", dependsOnMethods = "testBootstrapKafkaAdmin", timeOut = DEFAULT_TIMEOUT)
    public void testConfigureBrokerResource(String testName, ConfigResource.Type resourceType, AlterConfigOp.OpType opType) {
        LOGGER.info("kafka-config.sh {} <allowed>, script representation test", testName);
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.configureBrokerResource(resourceType, opType, "0")));
    }

    @Test(dependsOnMethods = "testTopicCreate", timeOut = DEFAULT_TIMEOUT)
    public void configTopicPermissionEvaluations() throws Throwable {

        LOGGER.info("kafka-config.sh --alter --entity-type topics --delete-config <allowed>, script representation test");
        var r = bwait(admin.configureBrokerResource(ConfigResource.Type.TOPIC, AlterConfigOp.OpType.DELETE, TOPIC_NAME));
        LOGGER.info("topic configured: {}", r);
    }

    @Test(dependsOnMethods = "testBootstrapKafkaAdmin", timeOut = DEFAULT_TIMEOUT)
    public void testGetConfigurationBroker() {

        LOGGER.info("kafka-configs.sh --describe --entity-type broker <forbidden>, script representation test");
        LOGGER.info("getting entity description for broker: 0");
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.getConfigurationBroker("0")));
    }

    @Test(dependsOnMethods = "testBootstrapKafkaAdmin", timeOut = DEFAULT_TIMEOUT)
    public void testConfigurationBrokerLogger() {

        LOGGER.info("kafka-configs.sh --describe --entity-type brokerLogger <forbidden>, script representation test");
        LOGGER.info("getting entity description for brokerLogger: 0");
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.getConfigurationBrokerLogger("0")));
    }

    @Test(dependsOnMethods = "testBootstrapKafkaAdmin", timeOut = DEFAULT_TIMEOUT)
    @Ignore
    public void testConfigurationUsersAlter() {

        LOGGER.info("kafka-configs.sh --alter --entity-type brokerLogger <permitted>, script representation test");
        // configuration-alter-users fail only due not previously existing configuration, but operation is allowed
        assertThrows(InvalidRequestException.class, () -> bwait(admin.alterConfigurationUser()));
    }

    @Test(dependsOnMethods = "testBootstrapKafkaAdmin", timeOut = DEFAULT_TIMEOUT)
    public void testConsumerGroupsList() throws Throwable {

        LOGGER.info("kafka-consumer-groups.sh --list <permitted>, script representation test");
        LOGGER.info("listing all consumer groups");
        var r = bwait(admin.listConsumerGroups());
        LOGGER.info("list consumer groups: {}", r);
    }

    @Test(dependsOnMethods = "testBootstrapKafkaAdmin", timeOut = DEFAULT_TIMEOUT)
    public void testConsumerGroupsDescribe() throws Throwable {

        LOGGER.info("kafka-consumer-groups.sh --group --describe <permitted>, script representation test");
        LOGGER.info("describing specific consumer group");
        var r = bwait(admin.describeConsumerGroups());
        LOGGER.info("describe consumer groups: {}", r);
    }

    @Test(dependsOnMethods = "testBootstrapKafkaAdmin", timeOut = DEFAULT_TIMEOUT)
    public void testConsumerGroupsDelete() {

        LOGGER.info("kafka-consumer-groups.sh --all-groups --delete  <permitted>, script representation test");
        LOGGER.info("deleting group");
        // should fail because the canary consumer group is not empty
        assertThrows(GroupNotEmptyException.class, () -> bwait(admin.deleteConsumerGroups()));
    }

    @Test(dependsOnMethods = "testBootstrapKafkaAdmin", timeOut = DEFAULT_TIMEOUT)
    public void testConsumerGroupsResetOffset() throws Throwable {

        LOGGER.info("kafka-consumer-groups.sh --all-groups --reset-offsets --execute --all-groups --all-topics  <permitted>, script representation test");
        bwait(admin.resetOffsets());
        LOGGER.info("offset successfully reset");
    }

    @Test(dependsOnMethods = "testBootstrapKafkaAdmin", timeOut = DEFAULT_TIMEOUT)
    public void testConsumerGroupsDeleteOffset() throws Throwable {

        LOGGER.info("kafka-consumer-groups.sh --delete-offsets  <permitted>, script representation test");
        bwait(admin.deleteOffset());
        LOGGER.info("offset successfully deleted");
    }

    @Test(dependsOnMethods = "testBootstrapKafkaAdmin", timeOut = DEFAULT_TIMEOUT)
    public void testDeleteRecord() throws Throwable {

        LOGGER.info("kafka-delete-records.sh --offset-json-file <permitted>, script representation test");
        bwait(admin.deleteRecords());
        LOGGER.info("record successfully deleted");
    }

    @Test(dependsOnMethods = "testBootstrapKafkaAdmin", timeOut = DEFAULT_TIMEOUT)
    public void testLeaderElectionUnclean() {

        LOGGER.info("kafka-leader-election.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.electLeader(ElectionType.UNCLEAN)));
    }

    @Test(dependsOnMethods = "testBootstrapKafkaAdmin", timeOut = DEFAULT_TIMEOUT)
    public void testLogDirsDesribe() {

        LOGGER.info("kafka-log-dirs.sh --describe <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.logDirs()));
    }

    @Test(dependsOnMethods = "testBootstrapKafkaAdmin", timeOut = DEFAULT_TIMEOUT)
    public void testLeaderElectionPreferred() {

        LOGGER.info("kafka-preferred-replica-election.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.electLeader(ElectionType.PREFERRED)));
    }

    @Test(dependsOnMethods = "testBootstrapKafkaAdmin", timeOut = DEFAULT_TIMEOUT)
    public void testReassignPartitions() {

        LOGGER.info("kafka-reassign-partitions.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.reassignPartitions()));
    }

    @Test(dependsOnMethods = "testBootstrapKafkaAdmin", timeOut = DEFAULT_TIMEOUT)
    public void testCreateDelegationToken() {

        LOGGER.info("kafka-delegation-tokens.sh create <forbidden>, script representation test");
        assertThrows(DelegationTokenDisabledException.class, () -> bwait(admin.createDelegationToken()));
    }

    @Test(dependsOnMethods = "testBootstrapKafkaAdmin", timeOut = DEFAULT_TIMEOUT)
    public void testDescribeDelegationToken() {

        LOGGER.info("kafka-delegation-tokens.sh describe <forbidden>, script representation test");
        assertThrows(DelegationTokenDisabledException.class, () -> bwait(admin.describeDelegationToken()));
    }
}