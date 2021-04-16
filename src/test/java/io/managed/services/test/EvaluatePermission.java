package io.managed.services.test;

import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.client.serviceapi.CreateServiceAccountPayload;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.client.serviceapi.ServiceAccount;
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;


import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteServiceAccountByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsReady;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(TestTag.SERVICE_API_PERMISSIONS)
@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EvaluatePermission {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPILongLiveTest.class);

    static final String KAFKA_INSTANCE_NAME = "mk-e2e-pe-" + Environment.KAFKA_POSTFIX_NAME;
    static final String SERVICE_ACCOUNT_NAME = "mk-e2e-sa-" + Environment.KAFKA_POSTFIX_NAME;
    static final String TOPIC_NAME = "test-topic-1";
    static final String PERSISTENT_TOPIC = "__strimzi_canary";


    ServiceAPI api;
    KafkaAdmin admin;

    KafkaResponse kafka;
    ServiceAccount serviceAccount;
    String topic;

    String bootstrapHost;
    String clientID;
    String clientSecret;

    @BeforeAll
    void  bootstrap(Vertx vertx, VertxTestContext context) {
        ServiceAPIUtils.serviceAPI(vertx)
                .onSuccess(a -> api = a)
                .onComplete(context.succeedingThenComplete());
    }


    @AfterAll
    void teardown(VertxTestContext context) {
        // close KafkaAdmin
        if (admin != null) admin.close();
        // delete service account
        deleteServiceAccountByNameIfExists(api, SERVICE_ACCOUNT_NAME)
                .compose(__ -> ServiceAPIUtils.deleteKafkaByNameIfExists(api, KAFKA_INSTANCE_NAME))
                .onComplete(context.succeedingThenComplete());
    }


    void assertAPI() {
        assumeTrue(api != null, "api is null because the bootstrap has failed");
    }

    void assertTopic() {
        assumeTrue(topic != null, "topic is null because the testCreateTopic has failed to create the topic on the Kafka instance");
    }

    void assertKafka() {
        assumeTrue(kafka != null, "kafka is null because the testPresenceOfLongLiveKafkaInstance has failed to create the Kafka instance");
    }

    void assertServiceAccount() {
        assumeTrue(serviceAccount != null, "serviceAccount is null because the testPresenceOfTheServiceAccount has failed to create the Service Account");
    }

    @Test
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    @Order(1)
    void testCreateKafkaInstance(Vertx vertx, VertxTestContext context) {
        assertAPI();

        // Create Kafka Instance
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        // add postfix to the name based on owner
        kafkaPayload.name = KAFKA_INSTANCE_NAME;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";

        LOGGER.info("create kafka instance: {}", kafkaPayload.name);
        api.createKafka(kafkaPayload, true)
                .compose(k -> waitUntilKafkaIsReady(vertx, api, k.id))
                .onSuccess(k -> kafka = k)
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    void testCreateServiceAccount(VertxTestContext context)  {
        assertAPI();
        assertKafka();

        // Create Service Account
        CreateServiceAccountPayload serviceAccountPayload = new CreateServiceAccountPayload();
        serviceAccountPayload.name = SERVICE_ACCOUNT_NAME;
        LOGGER.info("create service account: {}", serviceAccountPayload.name);
        api.createServiceAccount(serviceAccountPayload)
                .onSuccess(serviceAccountResponse -> {

                    serviceAccount = serviceAccountResponse;
                    bootstrapHost = kafka.bootstrapServerHost;
                    clientID = serviceAccount.clientID;
                    clientSecret = serviceAccount.clientSecret;
                    LOGGER.info("initialize kafka admin; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
                    admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);
                    topic = "f";


                })
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(3)
    @DisplayName("topics-create")
    void testTopicCreate(VertxTestContext context) {
        assertKafka();
        assertServiceAccount();
        LOGGER.info("kafka-topics.sh --create <Permitted>, script representation test");
        admin.createTopic(TOPIC_NAME)
                .onSuccess(__ -> {
                    LOGGER.info("topic successfully created: {}", TOPIC_NAME);
                    topic = TOPIC_NAME;
                })
                .onComplete(context.succeedingThenComplete());
    }
    @Test
    @Order(3)
    @DisplayName("topics-list")
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
    @Order(6)
    @DisplayName("topics-delete")
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
    @DisplayName("acl")
    @Order(4)
    List<DynamicTest> aclTests(VertxTestContext context) {
        assertAPI();
        assertKafka();
        return Arrays.asList(
                // --add
                DynamicTest.dynamicTest("--add --cluster", () -> aclPermissionEvaluations(context, "--add --cluster", ResourceType.CLUSTER)),
                DynamicTest.dynamicTest("--add --topic", () -> aclPermissionEvaluations(context, "--add --topic", ResourceType.TOPIC)),
                DynamicTest.dynamicTest("--add --group", () -> aclPermissionEvaluations(context, "--add --group", ResourceType.GROUP)),
                DynamicTest.dynamicTest("--add --delegation-token", () -> aclPermissionEvaluations(context, "--add --delegation-token", ResourceType.DELEGATION_TOKEN)),
                DynamicTest.dynamicTest("--add --transactional-id", () -> aclPermissionEvaluations(context, "--add --transactional-id", ResourceType.TRANSACTIONAL_ID)),

                // --list
                DynamicTest.dynamicTest("--list --topic", () -> aclListPermissionEvaluation(context, "--list --topic", ResourceType.TOPIC)),
                DynamicTest.dynamicTest("--list --cluster", () -> aclListPermissionEvaluation(context, "--list --cluster", ResourceType.CLUSTER)),
                DynamicTest.dynamicTest("--list --group", () -> aclListPermissionEvaluation(context, "--list --group", ResourceType.GROUP)),
                DynamicTest.dynamicTest("--list ", () -> aclListPermissionEvaluation(context, "--list ", ResourceType.ANY))
                );
    }


    void aclPermissionEvaluations(VertxTestContext context, String testName, ResourceType resourceType) {
        LOGGER.info(String.format("kafka-acls.sh %s <Forbidden>, script representation test", testName));
        admin.addAclResource(resourceType)
                .compose(r -> Future.failedFuture(String.format(" acl.sh  %s should fail due to not sufficient permissions", testName)))
                .recover(throwable -> {
                    if (throwable instanceof ClusterAuthorizationException) {
                        LOGGER.info(throwable.getMessage());
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }

    void aclListPermissionEvaluation(VertxTestContext context, String testName, ResourceType resourceType) {
        LOGGER.info(String.format("kafka-acls.sh %s <Forbidden>, script representation test", testName));
        admin.listAclResource(resourceType)
                .compose(r -> Future.failedFuture(String.format(" acl.sh  %s should fail due to not sufficient permissions", testName)))
                .recover(throwable -> {
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
    @Order(4)
    @DisplayName("configuration-describe-topic")
    void testConfigurationTopic(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-configs.sh --describe --entity-type topics <permitted>, script representation test");
        LOGGER.info("getting entity description for topic with name: {}", PERSISTENT_TOPIC);
        admin.getConfigurationTopic(PERSISTENT_TOPIC)
                .onSuccess(response -> LOGGER.info("response size: {}", response.size()))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(3)
    @DisplayName("configuration-describe-users")
    @Disabled
        // TODO should be allowed but fails
    void testConfigurationUsers(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-configs.sh --describe --entity-type brokerLogger <permitted>, script representation test");
        LOGGER.info("getting entity description for default user");
        admin.getConfigurationUserAll()
                .onSuccess(response -> LOGGER.info("user response: {}", response))
                .onComplete(context.succeedingThenComplete());
    }

    @TestFactory
    @DisplayName("configs")
    @Order(4)
    List<DynamicTest> configBroker(VertxTestContext context) {
        assertAPI();
        assertKafka();
        return Arrays.asList(
                // broker, broker-loggers
                DynamicTest.dynamicTest("brokers --add-config", () -> configTopicPermissionEvaluations(
                                context,
                                "brokers --add-config",
                                ConfigResource.Type.BROKER,
                                AlterConfigOp.OpType.APPEND)),
                DynamicTest.dynamicTest("brokers --delete-config", () -> configTopicPermissionEvaluations(
                                context,
                                "brokers --delete-config",
                                ConfigResource.Type.BROKER,
                                AlterConfigOp.OpType.DELETE)),
                DynamicTest.dynamicTest("broker-loggers --add-config", () -> configTopicPermissionEvaluations(
                                context,
                                "broker-loggers --add-config",
                                ConfigResource.Type.BROKER_LOGGER,
                                AlterConfigOp.OpType.DELETE)),
                DynamicTest.dynamicTest("broker-loggers --delete-config", () -> configTopicPermissionEvaluations(
                                context,
                                "broker-loggers --delete-config",
                                ConfigResource.Type.BROKER_LOGGER,
                                AlterConfigOp.OpType.DELETE))

        );
    }


    void configTopicPermissionEvaluations(VertxTestContext context, String testName, ConfigResource.Type resourceType, AlterConfigOp.OpType opType) {
        LOGGER.info(String.format("kafka-config.sh %s <allowed>, script representation test", testName));
        admin.configureBrokerResource(resourceType, opType, "0")
                .compose(r -> Future.failedFuture(String.format(" config.sh  %s should fail due to not sufficient permissions", testName)))
                .recover(throwable -> {
                    if (throwable instanceof ClusterAuthorizationException) {
                        LOGGER.info(throwable.getMessage());
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    @DisplayName("config topics --delete-config")
    void configTopicPermissionEvaluations(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-config.sh --alter --entity-type topics --delete-config <allowed>, script representation test");
        admin.configureBrokerResource(ConfigResource.Type.TOPIC, AlterConfigOp.OpType.DELETE, TOPIC_NAME)
                .onSuccess(response -> LOGGER.debug("Topic configured successfully"))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    @DisplayName("configuration-describe-broker")
    void testConfigurationBroker(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-configs.sh --describe --entity-type broker <forbidden>, script representation test");
        LOGGER.info("getting entity description for broker: 0");
        admin.getConfigurationBroker("0")
                .onSuccess(response -> {
                    LOGGER.debug("this behaviour shouldn't be possible");
                    LOGGER.info("response size: {}", response.size());
                })
                .compose(r -> Future.failedFuture(" configuration-describe-brokerLogger shouldn't be permitted  "))
                .recover(throwable -> {
                    if (throwable instanceof ClusterAuthorizationException) {
                        LOGGER.info(throwable.getMessage());
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })

                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    @DisplayName("configuration-describe-brokerLogger")
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
                .compose(r -> Future.failedFuture(" configuration-describe-brokerLogger shouldn't be permitted  "))
                .recover(throwable -> {
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
    @Disabled
    @DisplayName("configuration-alter-users")
    // TODO should be allowed but fails
    void testConfigurationUsersAlter(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-configs.sh --alter --entity-type brokerLogger <permitted>, script representation test");
        admin.alterConfigurationUser()
                .compose(r -> Future.failedFuture(" configuration-alter-users fail only due not previously existing configuration, but operation is allowed "))
                .recover(throwable -> {
                    if (throwable instanceof InvalidRequestException) {
                        LOGGER.info(throwable.getMessage());
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }


    @Test
    @Order(4)
    @DisplayName("consumer-groups-list")
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
    @Order(4)
    @DisplayName("consumer-groups-groups-describe")
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
    @Order(4)
    @DisplayName("consumer-groups-delete")
    void testConsumerGroupsDelete(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-consumer-groups.sh --all-groups --delete  <permitted>, script representation test");
        LOGGER.info("deleting group");
        admin.deleteConsumerGroups()
                .compose(r -> Future.failedFuture(" testConsumerGroupsDelete due to it's using of Canary group which is not empty "))
                .recover(throwable -> {
                    if (throwable instanceof GroupNotEmptyException) {
                        LOGGER.info(throwable.getMessage());
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    @DisplayName("consumer-groups-reset-consumer-group-offset")
    void testConsumerGroupsResetOffset(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-consumer-groups.sh --all-groups --reset-offsets --execute --all-groups --all-topics  <permitted>, script representation test");
        admin.resetOffsets()
                .onSuccess(__ -> LOGGER.info("offset successfully reset"))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    @DisplayName("consumer-groups--delete-offsets")
    void testConsumerGroupsDeleteOffset(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-consumer-groups.sh --delete-offsets  <permitted>, script representation test");
        admin.deleteOffset()
                .onSuccess(__ -> LOGGER.info("offset successfully deleted"))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    @DisplayName("delete-records-offset-json-file")
    void testDeleteRecord(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-delete-records.sh --offset-json-file <permitted>, script representation test");
        admin.deleteRecords()
                .onSuccess(__ -> LOGGER.info("record successfully deleted"))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    @DisplayName("leader-election--election-type UNCLEAN")
    void testLeaderElectionUnclean(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-leader-election.sh <forbidden>, script representation test");
        admin.electLeader(ElectionType.UNCLEAN)
                .compose(r -> Future.failedFuture(" Election is not needed "))
                .recover(throwable -> {
                    if (throwable instanceof ClusterAuthorizationException) {
                        LOGGER.info(throwable.getMessage());
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }


    @Test
    @Order(4)
    @DisplayName("log-dirs-describe")
    void testLogDirsDesribe(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-log-dirs.sh --describe <forbidden>, script representation test");
        admin.logDirs()
                .compose(r -> Future.failedFuture(" Attempt to read log directory on broker should not be permitted"))
                .recover(throwable -> {
                    if (throwable instanceof ClusterAuthorizationException) {
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    @DisplayName("preferred-replica-election")
    void testLeaderElectionPredered(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-preferred-replica-election.sh <forbidden>, script representation test");
        admin.electLeader(ElectionType.PREFERRED)
                .compose(r -> Future.failedFuture(" Election is not needed "))
                .recover(throwable -> {
                    if (throwable instanceof ClusterAuthorizationException) {
                        LOGGER.info(throwable.getMessage());
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    @DisplayName("reassign-partitions--generate and --execute")
    void testReassignPartitions(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-reassign-partitions.sh <forbidden>, script representation test");
        admin.reassignPartitions()
                .compose(r -> Future.failedFuture(" reassigning of partitions shouldn't be possible"))
                .recover(throwable -> {
                    if (throwable instanceof ClusterAuthorizationException) {
                        LOGGER.info(throwable.getMessage());
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    @DisplayName("delegation-tokens -create")
    void testCreateDelegationToken(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-delegation-tokens.sh create <forbidden>, script representation test");
        admin.createDelegationToken()
                .compose(r -> Future.failedFuture(" delegation-tokens -create should be forbidden"))
                .recover(throwable -> {
                    if (throwable instanceof DelegationTokenDisabledException) {
                        LOGGER.info(throwable.getMessage());
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    @DisplayName("delegation-tokens -describe")
    void testDescribeDelegationToken(VertxTestContext context) {
        assertAPI();
        assertKafka();
        LOGGER.info("kafka-delegation-tokens.sh describe <forbidden>, script representation test");
        admin.describeDelegationToken()
                .compose(r -> Future.failedFuture(" delegation-tokens -describe should be forbidden"))
                .recover(throwable -> {
                    if (throwable instanceof DelegationTokenDisabledException) {
                        LOGGER.info(throwable.getMessage());
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }


}