package io.managed.services.test;

import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.serviceapi.KafkaResponse;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.managed.services.test.TestUtils.bwait;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(TestTag.KAFKA_ADMIN_PERMISSIONS)
@Timeout(value = 5, unit = TimeUnit.MINUTES)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KafkaAdminPermissionTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(KafkaAdminPermissionTest.class);

    static final String KAFKA_INSTANCE_NAME = "mk-e2e-pe-" + Environment.KAFKA_POSTFIX_NAME;
    static final String SERVICE_ACCOUNT_NAME = "mk-e2e-pe-sa-" + Environment.KAFKA_POSTFIX_NAME;
    static final String TOPIC_NAME = "test-topic-1";
    static final String PERSISTENT_TOPIC = "__strimzi_canary";

    private final Vertx vertx = Vertx.vertx();

    ServiceAPI serviceAPI;
    KafkaAdmin admin;

    KafkaResponse kafka;
    String topic;

    @AfterAll
    void teardown() {
        // close KafkaAdmin
        if (admin != null) admin.close();

        assumeFalse(Environment.SKIP_TEARDOWN, "skip teardown");

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

    @BeforeAll
    void bootstrap() throws Throwable {
        serviceAPI = bwait(ServiceAPIUtils.serviceAPI(vertx));
    }

    void assertAPI() {
        assumeTrue(serviceAPI != null, "api is null because the bootstrap has failed");
    }

    void assertTopic() {
        assumeTrue(topic != null, "topic is null because the testCreateTopic has failed to create the topic on the Kafka instance");
    }

    void assertKafka() {
        assumeTrue(kafka != null, "kafka is null because the testPresenceOfLongLiveKafkaInstance has failed to create the Kafka instance");
    }

    @Test
    @Order(0)
    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    void testBootstrapKafkaAdmin() throws Throwable {

        kafka = bwait(ServiceAPIUtils.applyKafkaInstance(vertx, serviceAPI, KAFKA_INSTANCE_NAME));
        LOGGER.info("kafka instance connected/created: {}", kafka.name);

        var serviceAccount = bwait(ServiceAPIUtils.applyServiceAccount(serviceAPI, SERVICE_ACCOUNT_NAME));
        LOGGER.info("service account created/connected: {}", serviceAccount.name);

        String bootstrapHost = kafka.bootstrapServerHost;
        String clientID = serviceAccount.clientID;
        String clientSecret = serviceAccount.clientSecret;
        admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);
    }

    @Test
    @Order(1)
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void testTopicCreate() throws Throwable {
        assertKafka();

        LOGGER.info("kafka-topics.sh --create <Permitted>, script representation test");
        bwait(admin.createTopic(TOPIC_NAME));

        LOGGER.info("topic successfully created: {}", TOPIC_NAME);
        topic = TOPIC_NAME;
    }

    @Test
    @Order(2)
    void testTopicList() throws Throwable {
        assertAPI();
        assertKafka();
        assertTopic();

        LOGGER.info("kafka-topics.sh --list <Permitted>, script representation test");
        var r = bwait(admin.listTopics());

        LOGGER.info("topics successfully listed, response contains {} topic/s", r.size());
    }

    @Test
    @Order(4)
    void testTopicDelete() throws Throwable {
        assertAPI();
        assertKafka();
        assertTopic();

        LOGGER.info("kafka-topics.sh --delete <Permitted>, script representation test");
        LOGGER.info("delete created topic : {}", TOPIC_NAME);
        bwait(admin.deleteTopic(TOPIC_NAME));

        LOGGER.info("topic {} successfully deleted", TOPIC_NAME);
    }

    @TestFactory
    @Order(2)
    List<DynamicTest> testACL() {
        assertAPI();
        assertKafka();
        return Arrays.asList(
            // --add
            DynamicTest.dynamicTest("configAddCluster", () -> aclPermissionEvaluations("--add--cluster", ResourceType.CLUSTER)),
            DynamicTest.dynamicTest("configAddTopic", () -> aclPermissionEvaluations("--add--topic", ResourceType.TOPIC)),
            DynamicTest.dynamicTest("configAddGroup", () -> aclPermissionEvaluations("--add--group", ResourceType.GROUP)),
            DynamicTest.dynamicTest("configAddDelegationToken", () -> aclPermissionEvaluations("--add--delegation-token", ResourceType.DELEGATION_TOKEN)),
            DynamicTest.dynamicTest("configAddTransactionalId", () -> aclPermissionEvaluations("--ad--transactional-id", ResourceType.TRANSACTIONAL_ID)),

            // --list
            DynamicTest.dynamicTest("configListTopic", () -> aclListPermissionEvaluation("--list--topic", ResourceType.TOPIC)),
            DynamicTest.dynamicTest("configListCluster", () -> aclListPermissionEvaluation("--list--cluster", ResourceType.CLUSTER)),
            DynamicTest.dynamicTest("configListGroup", () -> aclListPermissionEvaluation("--list--group", ResourceType.GROUP)),
            DynamicTest.dynamicTest("configListAll", () -> aclListPermissionEvaluation("--list", ResourceType.ANY))
        );
    }


    private void aclPermissionEvaluations(String testName, ResourceType resourceType) {
        LOGGER.info("kafka-acls.sh {} <Forbidden>, script representation test", testName);
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.addAclResource(resourceType)));
    }

    private void aclListPermissionEvaluation(String testName, ResourceType resourceType) {
        LOGGER.info("kafka-acls.sh {} <Forbidden>, script representation test", testName);
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.listAclResource(resourceType)));
    }

    // kafka-configs.sh
    @Test
    @Order(2)
    void testGetConfigurationTopic() throws Throwable {
        assertAPI();
        assertKafka();

        LOGGER.info("kafka-configs.sh --describe --entity-type topics <permitted>, script representation test");
        LOGGER.info("getting entity description for topic with name: {}", PERSISTENT_TOPIC);
        var r = bwait(admin.getConfigurationTopic(PERSISTENT_TOPIC));
        LOGGER.info("response size: {}", r.size());
    }

    @Test
    @Order(2)
    @Disabled
        // TODO should be allowed but fails
    void testGetConfigurationUsers() throws Throwable {
        assertAPI();
        assertKafka();

        LOGGER.info("kafka-configs.sh --describe --entity-type brokerLogger <permitted>, script representation test");
        LOGGER.info("getting entity description for default user");
        var r = bwait(admin.getConfigurationUserAll());
        LOGGER.info("user response: {}", r);
    }

    @TestFactory
    @Order(2)
    List<DynamicTest> testConfigureBroker() {
        assertAPI();
        assertKafka();
        return Arrays.asList(
            // broker, broker-loggers
            DynamicTest.dynamicTest("configAddBroker", () -> configTopicPermissionEvaluations(
                "configAddBroker",
                ConfigResource.Type.BROKER,
                AlterConfigOp.OpType.APPEND)),
            DynamicTest.dynamicTest("configDeleteBroker", () -> configTopicPermissionEvaluations(
                "configDeleteBroker",
                ConfigResource.Type.BROKER,
                AlterConfigOp.OpType.DELETE)),
            DynamicTest.dynamicTest("configAddBrokerLogger", () -> configTopicPermissionEvaluations(
                "configAddBrokerLogger",
                ConfigResource.Type.BROKER_LOGGER,
                AlterConfigOp.OpType.DELETE)),
            DynamicTest.dynamicTest("configDeleteBrokerLogger", () -> configTopicPermissionEvaluations(
                "configDeleteBrokerLogger",
                ConfigResource.Type.BROKER_LOGGER,
                AlterConfigOp.OpType.DELETE))

        );
    }


    private void configTopicPermissionEvaluations(String testName, ConfigResource.Type resourceType, AlterConfigOp.OpType opType) {
        LOGGER.info("kafka-config.sh {} <allowed>, script representation test", testName);
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.configureBrokerResource(resourceType, opType, "0")));
    }

    @Test
    @Order(3)
    void configTopicPermissionEvaluations() throws Throwable {
        assertAPI();
        assertKafka();

        LOGGER.info("kafka-config.sh --alter --entity-type topics --delete-config <allowed>, script representation test");
        var r = bwait(admin.configureBrokerResource(ConfigResource.Type.TOPIC, AlterConfigOp.OpType.DELETE, TOPIC_NAME));
        LOGGER.info("topic configured: {}", r);
    }

    @Test
    @Order(2)
    void testGetConfigurationBroker() {
        assertAPI();
        assertKafka();

        LOGGER.info("kafka-configs.sh --describe --entity-type broker <forbidden>, script representation test");
        LOGGER.info("getting entity description for broker: 0");
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.getConfigurationBroker("0")));
    }

    @Test
    @Order(2)
    void testConfigurationBrokerLogger() {
        assertAPI();
        assertKafka();

        LOGGER.info("kafka-configs.sh --describe --entity-type brokerLogger <forbidden>, script representation test");
        LOGGER.info("getting entity description for brokerLogger: 0");
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.getConfigurationBrokerLogger("0")));
    }

    @Test
    @Order(2)
    @Disabled
        // TODO should be allowed but fails
    void testConfigurationUsersAlter() {
        assertAPI();
        assertKafka();

        LOGGER.info("kafka-configs.sh --alter --entity-type brokerLogger <permitted>, script representation test");
        // configuration-alter-users fail only due not previously existing configuration, but operation is allowed
        assertThrows(InvalidRequestException.class, () -> bwait(admin.alterConfigurationUser()));
    }


    @Test
    @Order(2)
    void testConsumerGroupsList() throws Throwable {
        assertAPI();
        assertKafka();

        LOGGER.info("kafka-consumer-groups.sh --list <permitted>, script representation test");
        LOGGER.info("listing all consumer groups");
        var r = bwait(admin.listConsumerGroups());
        LOGGER.info("list consumer groups: {}", r);
    }


    @Test
    @Order(2)
    void testConsumerGroupsDescribe() throws Throwable {
        assertAPI();
        assertKafka();

        LOGGER.info("kafka-consumer-groups.sh --group --describe <permitted>, script representation test");
        LOGGER.info("describing specific consumer group");
        var r = bwait(admin.describeConsumerGroups());
        LOGGER.info("describe consumer groups: {}", r);
    }


    @Test
    @Order(2)
    void testConsumerGroupsDelete() {
        assertAPI();
        assertKafka();

        LOGGER.info("kafka-consumer-groups.sh --all-groups --delete  <permitted>, script representation test");
        LOGGER.info("deleting group");
        // should fail because the canary consumer group is not empty
        assertThrows(GroupNotEmptyException.class, () -> bwait(admin.deleteConsumerGroups()));
    }

    @Test
    @Order(2)
    void testConsumerGroupsResetOffset() throws Throwable {
        assertAPI();
        assertKafka();

        LOGGER.info("kafka-consumer-groups.sh --all-groups --reset-offsets --execute --all-groups --all-topics  <permitted>, script representation test");
        bwait(admin.resetOffsets());
        LOGGER.info("offset successfully reset");
    }

    @Test
    @Order(2)
    void testConsumerGroupsDeleteOffset() throws Throwable {
        assertAPI();
        assertKafka();

        LOGGER.info("kafka-consumer-groups.sh --delete-offsets  <permitted>, script representation test");
        bwait(admin.deleteOffset());
        LOGGER.info("offset successfully deleted");
    }

    @Test
    @Order(2)
    void testDeleteRecord() throws Throwable {
        assertAPI();
        assertKafka();

        LOGGER.info("kafka-delete-records.sh --offset-json-file <permitted>, script representation test");
        bwait(admin.deleteRecords());
        LOGGER.info("record successfully deleted");
    }

    @Test
    @Order(2)
    void testLeaderElectionUnclean() {
        assertAPI();
        assertKafka();

        LOGGER.info("kafka-leader-election.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.electLeader(ElectionType.UNCLEAN)));
    }


    @Test
    @Order(2)
    void testLogDirsDesribe() {
        assertAPI();
        assertKafka();

        LOGGER.info("kafka-log-dirs.sh --describe <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.logDirs()));
    }

    @Test
    @Order(2)
    void testLeaderElectionPreferred() {
        assertAPI();
        assertKafka();

        LOGGER.info("kafka-preferred-replica-election.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.electLeader(ElectionType.PREFERRED)));
    }

    @Test
    @Order(2)
    void testReassignPartitions() {
        assertAPI();
        assertKafka();

        LOGGER.info("kafka-reassign-partitions.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> bwait(admin.reassignPartitions()));
    }

    @Test
    @Order(2)
    void testCreateDelegationToken() {
        assertAPI();
        assertKafka();

        LOGGER.info("kafka-delegation-tokens.sh create <forbidden>, script representation test");
        assertThrows(DelegationTokenDisabledException.class, () -> bwait(admin.createDelegationToken()));
    }

    @Test
    @Order(2)
    void testDescribeDelegationToken() {
        assertAPI();
        assertKafka();

        LOGGER.info("kafka-delegation-tokens.sh describe <forbidden>, script representation test");
        assertThrows(DelegationTokenDisabledException.class, () -> bwait(admin.describeDelegationToken()));
    }
}