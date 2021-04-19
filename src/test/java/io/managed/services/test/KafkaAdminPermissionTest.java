package io.managed.services.test;

import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
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
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.applyKafkaInstance;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.applyServiceAccount;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.serviceAPI;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(TestTag.KAFKA_ADMIN_PERMISSIONS)
@ExtendWith(VertxExtension.class)
@Timeout(value = 5, timeUnit = TimeUnit.MINUTES)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KafkaAdminPermissionTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(KafkaAdminPermissionTest.class);

    static final String KAFKA_INSTANCE_NAME = "mk-e2e-pe-" + Environment.KAFKA_POSTFIX_NAME;
    static final String SERVICE_ACCOUNT_NAME = "mk-e2e-pe-sa-" + Environment.KAFKA_POSTFIX_NAME;
    static final String TOPIC_NAME = "test-topic-1";
    static final String PERSISTENT_TOPIC = "__strimzi_canary";


    ServiceAPI serviceAPI;
    KafkaAdmin admin;

    KafkaResponse kafka;
    String topic;


    @AfterAll
    void teardown(VertxTestContext context) {
        // close KafkaAdmin
        if (admin != null) admin.close();

        assumeFalse(Environment.SKIP_TEARDOWN, "skip teardown");

        // delete service account
        ServiceAPIUtils.deleteServiceAccountByNameIfExists(serviceAPI, SERVICE_ACCOUNT_NAME)

            // delete kafka instance
            .compose(__ -> ServiceAPIUtils.deleteKafkaByNameIfExists(serviceAPI, KAFKA_INSTANCE_NAME))

            .onComplete(context.succeedingThenComplete());
    }

    @BeforeAll
    void bootstrap(Vertx vertx, VertxTestContext context) {
        serviceAPI(vertx)
            .onSuccess(a -> serviceAPI = a)
            .onComplete(context.succeedingThenComplete());
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
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testBootstrapKafkaAdmin(Vertx vertx, VertxTestContext context) {

        applyKafkaInstance(vertx, serviceAPI, KAFKA_INSTANCE_NAME)
            .compose(k -> {
                LOGGER.info("kafka instance connected/created: {}", k.name);
                kafka = k;

                return applyServiceAccount(serviceAPI, SERVICE_ACCOUNT_NAME);
            })

            .onSuccess(serviceAccount -> {
                LOGGER.info("service account created/connected: {}", serviceAccount.name);
                String bootstrapHost = kafka.bootstrapServerHost;
                String clientID = serviceAccount.clientID;
                String clientSecret = serviceAccount.clientSecret;
                admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);
            })

            .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(1)
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    void testTopicCreate(VertxTestContext context) {
        assertKafka();

        LOGGER.info("kafka-topics.sh --create <Permitted>, script representation test");
        admin.createTopic(TOPIC_NAME)
                .onSuccess(__ -> {
                    LOGGER.info("topic successfully created: {}", TOPIC_NAME);
                    topic = TOPIC_NAME;
                })
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    void testTopicList(VertxTestContext context) {
        assertAPI();
        assertKafka();
        assertTopic();
        LOGGER.info("kafka-topics.sh --list <Permitted>, script representation test");
        admin.listTopics()
                .onSuccess(r -> LOGGER.info("topics successfully listed, response contains {} topic/s", r.size()))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    void testTopicDelete(VertxTestContext context) {
        assertAPI();
        assertKafka();
        assertTopic();
        LOGGER.info("kafka-topics.sh --delete <Permitted>, script representation test");
        LOGGER.info("Delete created topic : {}", TOPIC_NAME);
        admin.deleteTopic(TOPIC_NAME)
                .onSuccess(__ -> LOGGER.info("topic {} successfully deleted", TOPIC_NAME))
                .onComplete(context.succeedingThenComplete());
    }

    @TestFactory
    @Order(2)
    List<DynamicTest> testACL(VertxTestContext context) {
        assertAPI();
        assertKafka();
        return Arrays.asList(
                // --add
                DynamicTest.dynamicTest("configAddCluster", () -> aclPermissionEvaluations(context, "--add--cluster", ResourceType.CLUSTER)),
                DynamicTest.dynamicTest("configAddTopic", () -> aclPermissionEvaluations(context, "--add--topic", ResourceType.TOPIC)),
                DynamicTest.dynamicTest("configAddGroup", () -> aclPermissionEvaluations(context, "--add--group", ResourceType.GROUP)),
                DynamicTest.dynamicTest("configAddDelegationToken", () -> aclPermissionEvaluations(context, "--add--delegation-token", ResourceType.DELEGATION_TOKEN)),
                DynamicTest.dynamicTest("configAddTransactionalId", () -> aclPermissionEvaluations(context, "--ad--transactional-id", ResourceType.TRANSACTIONAL_ID)),

                // --list
                DynamicTest.dynamicTest("configListTopic", () -> aclListPermissionEvaluation(context, "--list--topic", ResourceType.TOPIC)),
                DynamicTest.dynamicTest("configListCluster", () -> aclListPermissionEvaluation(context, "--list--cluster", ResourceType.CLUSTER)),
                DynamicTest.dynamicTest("configListGroup", () -> aclListPermissionEvaluation(context, "--list--group", ResourceType.GROUP)),
                DynamicTest.dynamicTest("configListAll", () -> aclListPermissionEvaluation(context, "--list", ResourceType.ANY))
        );
    }


    private void aclPermissionEvaluations(VertxTestContext context, String testName, ResourceType resourceType) {
        LOGGER.info("kafka-acls.sh {} <Forbidden>, script representation test", testName);
        admin.addAclResource(resourceType)
                .compose(r ->
                    Future.failedFuture(String.format(" acl.sh  %s should fail due to not sufficient permissions", testName)),
                    throwable -> {
                        if (throwable instanceof ClusterAuthorizationException) {
                            LOGGER.info(throwable.getMessage());
                            return Future.succeededFuture();
                        }
                        return Future.failedFuture(throwable);
                    }
                )
                .onComplete(context.succeedingThenComplete());
    }

    private void aclListPermissionEvaluation(VertxTestContext context, String testName, ResourceType resourceType) {
        LOGGER.info("kafka-acls.sh {} <Forbidden>, script representation test", testName);
        admin.listAclResource(resourceType)
                .compose(r ->
                    Future.failedFuture(String.format(" acl.sh  %s should fail due to not sufficient permissions", testName)),
                    throwable -> {
                        if (throwable instanceof ClusterAuthorizationException) {
                            LOGGER.info(throwable.getMessage());
                            return Future.succeededFuture();
                        }
                        return Future.failedFuture(throwable);
                    })
                .onComplete(context.succeedingThenComplete());
    }

    // kafka-configs.sh
    @Test
    @Order(2)
    void testGetConfigurationTopic(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-configs.sh --describe --entity-type topics <permitted>, script representation test");
        LOGGER.info("getting entity description for topic with name: {}", PERSISTENT_TOPIC);
        admin.getConfigurationTopic(PERSISTENT_TOPIC)
                .onSuccess(response -> LOGGER.info("response size: {}", response.size()))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    @Disabled
        // TODO should be allowed but fails
    void testGetConfigurationUsers(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-configs.sh --describe --entity-type brokerLogger <permitted>, script representation test");
        LOGGER.info("getting entity description for default user");
        admin.getConfigurationUserAll()
                .onSuccess(response -> LOGGER.info("user response: {}", response))
                .onComplete(context.succeedingThenComplete());
    }

    @TestFactory
    @Order(2)
    List<DynamicTest> testConfigureBroker(VertxTestContext context) {
        assertAPI();
        assertKafka();
        return Arrays.asList(
                // broker, broker-loggers
                DynamicTest.dynamicTest("configAddBroker", () -> configTopicPermissionEvaluations(
                                context,
                                "configAddBroker",
                                ConfigResource.Type.BROKER,
                                AlterConfigOp.OpType.APPEND)),
                DynamicTest.dynamicTest("configDeleteBroker", () -> configTopicPermissionEvaluations(
                                context,
                                "configDeleteBroker",
                                ConfigResource.Type.BROKER,
                                AlterConfigOp.OpType.DELETE)),
                DynamicTest.dynamicTest("configAddBrokerLogger", () -> configTopicPermissionEvaluations(
                                context,
                                "configAddBrokerLogger",
                                ConfigResource.Type.BROKER_LOGGER,
                                AlterConfigOp.OpType.DELETE)),
                DynamicTest.dynamicTest("configDeleteBrokerLogger", () -> configTopicPermissionEvaluations(
                                context,
                                "configDeleteBrokerLogger",
                                ConfigResource.Type.BROKER_LOGGER,
                                AlterConfigOp.OpType.DELETE))

        );
    }


    private void configTopicPermissionEvaluations(VertxTestContext context, String testName, ConfigResource.Type resourceType, AlterConfigOp.OpType opType) {
        LOGGER.info("kafka-config.sh {} <allowed>, script representation test", testName);
        admin.configureBrokerResource(resourceType, opType, "0")
                .compose(r ->
                    Future.failedFuture(String.format(" config.sh  %s should fail due to not sufficient permissions", testName)),
                    throwable -> {
                        if (throwable instanceof ClusterAuthorizationException) {
                            LOGGER.info(throwable.getMessage());
                            return Future.succeededFuture();
                        }
                        return Future.failedFuture(throwable);
                    })
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(3)
    void configTopicPermissionEvaluations(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-config.sh --alter --entity-type topics --delete-config <allowed>, script representation test");
        admin.configureBrokerResource(ConfigResource.Type.TOPIC, AlterConfigOp.OpType.DELETE, TOPIC_NAME)
                .onSuccess(response -> LOGGER.debug("Topic configured successfully"))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    void testGetConfigurationBroker(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-configs.sh --describe --entity-type broker <forbidden>, script representation test");
        LOGGER.info("getting entity description for broker: 0");
        admin.getConfigurationBroker("0")
                .onSuccess(response -> {
                    LOGGER.debug("this behaviour shouldn't be possible");
                    LOGGER.info("response size: {}", response.size());
                })
                .compose(r ->
                    Future.failedFuture("configuration-describe-brokerLogger shouldn't be permitted"),
                    throwable -> {
                        if (throwable instanceof ClusterAuthorizationException) {
                            LOGGER.info(throwable.getMessage());
                            return Future.succeededFuture();
                        }
                        return Future.failedFuture(throwable);
                    })

                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    void testConfigurationBrokerLogger(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-configs.sh --describe --entity-type brokerLogger <forbidden>, script representation test");
        LOGGER.info("getting entity description for brokerLogger: 0");
        admin.getConfigurationBrokerLogger("0")
                .onSuccess(response -> {
                    LOGGER.debug("this behaviour shouldn't be possible");
                    LOGGER.info("response size: {}", response.size());
                })
                .compose(r ->
                    Future.failedFuture("configuration-describe-brokerLogger shouldn't be permitted"),
                    throwable -> {
                        if (throwable instanceof ClusterAuthorizationException) {
                            LOGGER.info(throwable.getMessage());
                            return Future.succeededFuture();
                        }
                        return Future.failedFuture(throwable);
                    })

                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    @Disabled
    // TODO should be allowed but fails
    void testConfigurationUsersAlter(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-configs.sh --alter --entity-type brokerLogger <permitted>, script representation test");
        admin.alterConfigurationUser()
                .compose(r ->
                    Future.failedFuture("configuration-alter-users fail only due not previously existing configuration, but operation is allowed "),
                    throwable -> {
                        if (throwable instanceof InvalidRequestException) {
                            LOGGER.info(throwable.getMessage());
                            return Future.succeededFuture();
                        }
                        return Future.failedFuture(throwable);
                    })
                .onComplete(context.succeedingThenComplete());
    }


    @Test
    @Order(2)
    void testConsumerGroupsList(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-consumer-groups.sh --list <permitted>, script representation test");
        LOGGER.info("listing all consumer groups");
        admin.listConsumerGroups()
                .onSuccess(response -> LOGGER.info("response size: {}", response.size()))
                .onComplete(context.succeedingThenComplete());
    }


    @Test
    @Order(2)
    void testConsumerGroupsDescribe(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-consumer-groups.sh --group --describe <permitted>, script representation test");
        LOGGER.info("describing specific consumer group");
        admin.describeConsumerGroups()
                .onSuccess(response -> LOGGER.info("response size: {}", response.size()))
                .onComplete(context.succeedingThenComplete());
    }


    @Test
    @Order(2)
    void testConsumerGroupsDelete(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-consumer-groups.sh --all-groups --delete  <permitted>, script representation test");
        LOGGER.info("deleting group");
        admin.deleteConsumerGroups()
                .compose(r ->
                    Future.failedFuture(" testConsumerGroupsDelete due to it's using of Canary group which is not empty "),
                    throwable -> {
                        if (throwable instanceof GroupNotEmptyException) {
                            LOGGER.info(throwable.getMessage());
                            return Future.succeededFuture();
                        }
                        return Future.failedFuture(throwable);
                    })
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    void testConsumerGroupsResetOffset(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-consumer-groups.sh --all-groups --reset-offsets --execute --all-groups --all-topics  <permitted>, script representation test");
        admin.resetOffsets()
                .onSuccess(__ -> LOGGER.info("offset successfully reset"))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    void testConsumerGroupsDeleteOffset(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-consumer-groups.sh --delete-offsets  <permitted>, script representation test");
        admin.deleteOffset()
                .onSuccess(__ -> LOGGER.info("offset successfully deleted"))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    void testDeleteRecord(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-delete-records.sh --offset-json-file <permitted>, script representation test");
        admin.deleteRecords()
                .onSuccess(__ -> LOGGER.info("record successfully deleted"))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    void testLeaderElectionUnclean(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-leader-election.sh <forbidden>, script representation test");
        admin.electLeader(ElectionType.UNCLEAN)
                .compose(r ->
                    Future.failedFuture(" Election is not needed "),
                    throwable -> {
                        if (throwable instanceof ClusterAuthorizationException) {
                            LOGGER.info(throwable.getMessage());
                            return Future.succeededFuture();
                        }
                        return Future.failedFuture(throwable);
                    })
                .onComplete(context.succeedingThenComplete());
    }


    @Test
    @Order(2)
    void testLogDirsDesribe(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-log-dirs.sh --describe <forbidden>, script representation test");
        admin.logDirs()
                .compose(r ->
                    Future.failedFuture(" Attempt to read log directory on broker should not be permitted"),
                    throwable -> {
                        if (throwable instanceof ClusterAuthorizationException) {
                            return Future.succeededFuture();
                        }
                        return Future.failedFuture(throwable);
                    })
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    void testLeaderElectionPreferred(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-preferred-replica-election.sh <forbidden>, script representation test");
        admin.electLeader(ElectionType.PREFERRED)
            .compose(r ->
                Future.failedFuture(" Election is not needed "),
                throwable -> {
                    if (throwable instanceof ClusterAuthorizationException) {
                        LOGGER.info(throwable.getMessage());
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })
            .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    void testReassignPartitions(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-reassign-partitions.sh <forbidden>, script representation test");
        admin.reassignPartitions()
                .compose(r ->
                    Future.failedFuture(" reassigning of partitions shouldn't be possible"),
                    throwable -> {
                        if (throwable instanceof ClusterAuthorizationException) {
                            LOGGER.info(throwable.getMessage());
                            return Future.succeededFuture();
                        }
                        return Future.failedFuture(throwable);
                    })
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    void testCreateDelegationToken(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-delegation-tokens.sh create <forbidden>, script representation test");
        admin.createDelegationToken()
                .compose(r ->
                    Future.failedFuture(" delegation-tokens -create should be forbidden"),
                    throwable -> {
                        if (throwable instanceof DelegationTokenDisabledException) {
                            LOGGER.info(throwable.getMessage());
                            return Future.succeededFuture();
                        }
                        return Future.failedFuture(throwable);
                    })
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    void testDescribeDelegationToken(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-delegation-tokens.sh describe <forbidden>, script representation test");
        admin.describeDelegationToken()
                .compose(r ->
                    Future.failedFuture("delegation-tokens -describe should be forbidden"),
                    throwable -> {
                        if (throwable instanceof DelegationTokenDisabledException) {
                            LOGGER.info(throwable.getMessage());
                            return Future.succeededFuture();
                        }
                        return Future.failedFuture(throwable);
                    })
                .onComplete(context.succeedingThenComplete());
    }
}