package io.managed.services.test.kafka;

import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.oauth.KeycloakLoginSession;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.DelegationTokenDisabledException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.resource.ResourceType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

/**
 * Test the configured ACLs for a Kafka Instance using the Kafka Admin[1] library which is the same Java library used
 * by the kafka bin scripts.
 * <p>
 * 1. https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/admin/Admin.java
 * <p>
 * <b>Requires:</b>
 * <ul>
 *     <li> PRIMARY_USERNAME
 *     <li> PRIMARY_PASSWORD
 * </ul>
 */
@Log4j2
public class KafkaAdminPermissionTest extends TestBase {
    private static final String KAFKA_INSTANCE_NAME = "mk-e2e-pe-" + Environment.LAUNCH_KEY;
    private static final String SERVICE_ACCOUNT_NAME = "mk-e2e-pe-sa-" + Environment.LAUNCH_KEY;
    private static final String TOPIC_NAME = "test-topic-1";

    private static final String TEST_GROUP_ID = "temporary-group-id";
    private static final String TOPIC_NAME_FOR_GROUPS = "temporary-topic-name";

    private final Vertx vertx = Vertx.vertx();

    private KafkaMgmtApi kafkaMgmtApi;
    private SecurityMgmtApi securityMgmtApi;
    private KafkaInstanceApi kafkaInstanceApi;
    private KafkaAdmin admin;

    @AfterClass(alwaysRun = true)
    @SneakyThrows
    public void teardown() {
        if (admin != null) {
            // close KafkaAdmin
            admin.close();
        }

        assumeTeardown();

        // delete temporary topic for test concerned about groups.
        try {
            kafkaInstanceApi.deleteTopic(TOPIC_NAME_FOR_GROUPS);
        } catch (Throwable t) {
            log.error("error deleting temporary topic: ", t);
        }

        // delete service account
        try {
            SecurityMgmtAPIUtils.cleanServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            log.error("clean service account error: ", t);
        }

        // delete kafka instance
        try {
            KafkaMgmtApiUtils.cleanKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            log.error("clean kafka error: ", t);
        }

        // close vertx
        bwait(vertx.close());
    }

    @BeforeClass
    public void bootstrap() throws Throwable {
        assertNotNull(Environment.PRIMARY_USERNAME, "the PRIMARY_USERNAME env is null");
        assertNotNull(Environment.PRIMARY_PASSWORD, "the PRIMARY_PASSWORD env is null");

        var auth = new KeycloakLoginSession(Environment.PRIMARY_USERNAME, Environment.PRIMARY_PASSWORD);
        var redhatUser = bwait(auth.loginToRedHatSSO());

        kafkaMgmtApi = KafkaMgmtApiUtils.kafkaMgmtApi(Environment.OPENSHIFT_API_URI, redhatUser);
        securityMgmtApi = SecurityMgmtAPIUtils.securityMgmtApi(Environment.OPENSHIFT_API_URI, redhatUser);

        var kafka = KafkaMgmtApiUtils.applyKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);

        var serviceAccount = SecurityMgmtAPIUtils.applyServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME);

        var masUser = bwait(auth.loginToOpenshiftIdentity());
        kafkaInstanceApi = KafkaInstanceApiUtils.kafkaInstanceApi(kafka, masUser);

        // create topic
        KafkaInstanceApiUtils.applyTopic(kafkaInstanceApi, TOPIC_NAME_FOR_GROUPS);

        // create the kafka admin
        admin = new KafkaAdmin(
            kafka.getBootstrapServerHost(),
            serviceAccount.getClientId(),
            serviceAccount.getClientSecret());
        log.info("kafka admin api initialized for instance: {}", kafka.getBootstrapServerHost());
    }

    @Test
    public void testForbiddenToCreateTopic() {
        log.info("kafka-topics.sh --create <forbidden>, script representation test");
        assertThrows(TopicAuthorizationException.class, () -> admin.createTopic(TOPIC_NAME));
    }

    @Test
    public void testAllowedToListTopic() {
        log.info("kafka-topics.sh --list <permitted>, script representation test");
        admin.listTopics();
    }

    @Test
    public void testForbiddenToDeleteTopic() {
        log.info("kafka-topics.sh --delete <forbidden>, script representation test");
        assertThrows(TopicAuthorizationException.class, () -> admin.deleteTopic(TOPIC_NAME));
    }

    @DataProvider
    public Object[][] aclAddCmdProvider() {
        return new Object[][] {
            {"--add--cluster", ResourceType.CLUSTER},
            {"--add--topic", ResourceType.TOPIC},
            {"--add--group", ResourceType.GROUP},
            {"--add--delegation-token", ResourceType.DELEGATION_TOKEN},
            {"--add--transactional-id", ResourceType.TRANSACTIONAL_ID},
        };
    }

    @Test(dataProvider = "aclAddCmdProvider")
    public void testForbiddenToAlterACLResource(String testName, ResourceType resourceType) {
        log.info("kafka-acls.sh {} <Forbidden>, script representation test", testName);
        assertThrows(ClusterAuthorizationException.class, () -> admin.addAclResource(resourceType));
    }

    @DataProvider
    public Object[][] aclListCmdProvider() {
        return new Object[][] {
            {"--list--topic", ResourceType.TOPIC},
            {"--list--cluster", ResourceType.CLUSTER},
            {"--list--group", ResourceType.GROUP},
            {"--list--transactional-id", ResourceType.TRANSACTIONAL_ID},
            {"--list--delegation-token", ResourceType.DELEGATION_TOKEN},
            {"--list", ResourceType.ANY},
        };
    }

    @Test(dataProvider = "aclListCmdProvider")
    public void testAllowedToListACLResource(String testName, ResourceType resourceType) {
        log.info("kafka-acls.sh {} <Forbidden>, script representation test", testName);
        var r = admin.listAclResource(resourceType);
        log.info("response size: {}", r.size());
    }

    @DataProvider
    public Object[][] aclDeleteCmdProvider() {
        return new Object[][] {
            {"--delete--topic", ResourceType.TOPIC},
            {"--delete--cluster", ResourceType.CLUSTER},
            {"--delete--group", ResourceType.GROUP},
            {"--delete--transactional-id", ResourceType.TRANSACTIONAL_ID},
            {"--delete--delegation-token", ResourceType.DELEGATION_TOKEN},
            {"--delete", ResourceType.ANY},
        };
    }

    @Test(dataProvider = "aclDeleteCmdProvider")
    public void testForbiddenToDeleteACLResource(String testName, ResourceType resourceType) {
        log.info("kafka-acls.sh {} <forbidden>, script representation test", testName);
        assertThrows(ClusterAuthorizationException.class, () -> admin.deleteAclResource(resourceType));
    }

    @Test
    public void testAllowedToDescribeTopicConfiguration() {

        log.info("kafka-configs.sh --describe --entity-type topics <permitted>, script representation test");
        log.info("getting entity description for topic with name: {}", TOPIC_NAME_FOR_GROUPS);
        var r = admin.getConfigurationTopic(TOPIC_NAME_FOR_GROUPS);
        log.info("response size: {}", r.size());
    }

    @Test
    @Ignore
    public void testAllowedToDescribeUserConfiguration() {

        log.info("kafka-configs.sh --describe --entity-type brokerLogger <permitted>, script representation test");
        log.info("getting entity description for default user");
        var r = admin.getConfigurationUserAll();
        log.info("user response: {}", r);
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

    @Test(dataProvider = "configureBrokerCmdProvider")
    public void testForbiddenToAlterBrokerConfig(String testName, ConfigResource.Type resourceType, AlterConfigOp.OpType opType) {
        log.info("kafka-config.sh {} <forbidden>, script representation test", testName);
        assertThrows(ClusterAuthorizationException.class, () -> admin.configureBrokerResource(resourceType, opType, "0"));
    }

    @Test()
    public void testForbiddenToDeleteTopicConfig() {
        log.info("kafka-config.sh --alter --entity-type topics --delete-config <forbidden>, script representation test");
        assertThrows(TopicAuthorizationException.class, () -> admin.configureBrokerResource(ConfigResource.Type.TOPIC, AlterConfigOp.OpType.DELETE, TOPIC_NAME));
    }

    @Test
    public void testForbiddenToDescribeBrokerConfig() {

        log.info("kafka-configs.sh --describe --entity-type broker <forbidden>, script representation test");
        log.info("getting entity description for broker: 0");
        assertThrows(ClusterAuthorizationException.class, () -> admin.getConfigurationBroker("0"));
    }

    @Test
    public void testForbiddenToDescribeBrokerLoggerConfig() {

        log.info("kafka-configs.sh --describe --entity-type brokerLogger <forbidden>, script representation test");
        log.info("getting entity description for brokerLogger: 0");
        assertThrows(ClusterAuthorizationException.class, () -> admin.getConfigurationBrokerLogger("0"));
    }

    @Test
    @Ignore
    public void testForbiddenToAlterUserConfig() {

        log.info("kafka-configs.sh --alter --entity-type brokerLogger <permitted>, script representation test");
        // configuration-alter-users fail only due not previously existing configuration, but operation is allowed
        assertThrows(InvalidRequestException.class, () -> admin.alterConfigurationUser());
    }

    @Test
    public void testAllowedToListConsumerGroups() {

        log.info("kafka-consumer-groups.sh --list <permitted>, script representation test");
        log.info("listing all consumer groups");
        var r = admin.listConsumerGroups();
        log.info("list consumer groups: {}", r);
    }

    @Test
    public void testAllowedToDescribeConsumerGroup() {

        log.info("kafka-consumer-groups.sh --group --describe <permitted>, script representation test");
        log.info("describing specific consumer group");
        var r = admin.describeConsumerGroups(TEST_GROUP_ID);
        log.info("describe consumer groups: {}", r);
    }

    @Test
    public void testForbiddenToDeleteConsumerGroup() {
        log.info("kafka-consumer-groups.sh --all-groups --delete  <forbidden>, script representation test");
        assertThrows(GroupAuthorizationException.class, () -> admin.deleteConsumerGroups(TEST_GROUP_ID));
    }

    @Test
    public void testForbiddenToResetConsumerGroupOffset() {
        log.info("kafka-consumer-groups.sh --all-groups --reset-offsets --execute --all-groups --all-topics  <forbidden>, script representation test");
        assertThrows(GroupAuthorizationException.class, () -> admin.resetOffsets(TOPIC_NAME_FOR_GROUPS, TEST_GROUP_ID));
    }

    @Test
    public void testForbiddenToDeleteConsumerGroupOffset() {

        log.info("kafka-consumer-groups.sh --delete-offsets  <forbidden>, script representation test");
        assertThrows(GroupAuthorizationException.class, () -> admin.deleteOffset(TOPIC_NAME_FOR_GROUPS, TEST_GROUP_ID));
    }

    @Test
    public void testForbiddenToDeleteRecords() {

        log.info("kafka-delete-records.sh --offset-json-file <forbidden>, script representation test");
        assertThrows(TopicAuthorizationException.class, () -> admin.deleteRecords(TOPIC_NAME_FOR_GROUPS));
    }

    @Test
    public void testForbiddenToUncleanLeaderElection() {

        log.info("kafka-leader-election.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> admin.electLeader(ElectionType.UNCLEAN, TOPIC_NAME_FOR_GROUPS));
    }

    @Test
    public void testForbiddenToDescribeLogDirs() {

        log.info("kafka-log-dirs.sh --describe <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> admin.logDirs());
    }

    @Test
    public void testForbiddenToAlterPreferredReplicaElection() {

        log.info("kafka-preferred-replica-election.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> admin.electLeader(ElectionType.PREFERRED, TOPIC_NAME_FOR_GROUPS));
    }

    @Test
    public void testForbiddenToReassignPartitions() {

        log.info("kafka-reassign-partitions.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> admin.reassignPartitions(TOPIC_NAME_FOR_GROUPS));
    }

    @Test
    public void testForbiddenToCreateDelegationToken() {

        log.info("kafka-delegation-tokens.sh create <forbidden>, script representation test");
        assertThrows(DelegationTokenDisabledException.class, () -> admin.createDelegationToken());
    }

    @Test
    public void testForbiddenToDescribeDelegationToken() {

        log.info("kafka-delegation-tokens.sh describe <forbidden>, script representation test");
        assertThrows(DelegationTokenDisabledException.class, () -> admin.describeDelegationToken());
    }
}