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
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.errors.DelegationTokenDisabledException;
import org.apache.kafka.common.errors.ElectionNotNeededException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.TimeUnit;


import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsReady;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteServiceAccountByNameIfExists;

@Tag(TestTag.SERVICE_API_PERMISSIONS)
@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EvaluatePermission {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPILongLiveTest.class);

    static final String KAFKA_INSTANCE_NAME = "mk-e2e-pe-" + Environment.KAFKA_POSTFIX_NAME;
    static final String SERVICE_ACCOUNT_NAME = "mk-e2e-sa-" + Environment.KAFKA_POSTFIX_NAME;
    static final String TOPIC_NAME = "test-topic-1";
    static final String PERSISTANT_TOPIC = "strimzi-canary";


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
        assertTopic();
        LOGGER.info("kafka-topics.sh --list <Permitted>, script representation test");
        admin.listTopics()
                .onSuccess(r -> LOGGER.info("topics successfully listed, response contains {} topic/s", r.size()))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    @DisplayName("topics-delete")
    void testTopicDelete(VertxTestContext context) {
        assertTopic();
        LOGGER.info("kafka-topics.sh --delete <Permitted>, script representation test");
        LOGGER.info("Delete created topic : {}", TOPIC_NAME);
        admin.deleteTopic(TOPIC_NAME)
                .onSuccess(__ -> LOGGER.info("topic {} successfully deleted", TOPIC_NAME))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    @DisplayName("acls-add-topic")
    void setUpAcl(VertxTestContext context) {
        LOGGER.info("kafka-acls.sh --add --topic <Forbiddden>, script representation test");
        admin.createAclTopic()
                .compose(r -> Future.failedFuture(" add acls topic should fail due to not sufficient permissions"))
                .recover(throwable -> {
                    if (throwable instanceof SecurityDisabledException) {
                        LOGGER.info(throwable.getMessage());
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }
    @Test
    @Order(4)
    @DisplayName("acls-list-topic")
    void testAcl(VertxTestContext context) {
        LOGGER.info("kafka-acls.sh --list --topic <Forbidden>, script representation test");
        admin.listAclTopic()
                .compose(r -> Future.failedFuture(" acls listing topics should fail due to not sufficient permissions"))
                .recover(throwable -> {
                    if (throwable instanceof SecurityDisabledException) {
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
        LOGGER.info("kafka-configs.sh --describe --entity-type topics <permitted>, script representation test");
        LOGGER.info("getting entity description for topic with name: {}", PERSISTANT_TOPIC);
        admin.getConfigurationTopic(PERSISTANT_TOPIC)
                .onSuccess(response -> LOGGER.info("response size: {}", response.size()))
                .onComplete(context.succeedingThenComplete());
    }

    // TODO should go but response is empty
    @Test
    @Order(3)
    @DisplayName("configuration-describe-users")
    void testConfigurationUsers(VertxTestContext context) {
        LOGGER.info("kafka-configs.sh --describe --entity-type brokerLogger <permitted>, script representation test");
        LOGGER.info("getting entity description for default user");
        admin.getConfigurationUserAll()
                .onSuccess(response -> LOGGER.info("user response: {}", response))
                .onComplete(context.succeedingThenComplete());
    }


    @Test
    @Order(4)
    @DisplayName("configuration-describe-broker")
    // TODO should fail but doesn't
    // possible broker identifiers: "0", "1", "2"
    void testConfigurationBroker(VertxTestContext context) {
        LOGGER.info("kafka-configs.sh --describe --entity-type broker <forbidden>, script representation test");
        LOGGER.info("getting entity description for broker: 0");
        admin.getConfigurationBroker("0")
                .onSuccess(response -> {
                    LOGGER.debug("this behaviour shouldn't be possible");
                    LOGGER.info("response size: {}", response.size());
                })
                .compose(r -> Future.failedFuture(" configuration-describe-brokerLogger shouldn't be permitted  "))
                .recover(throwable -> {
                    // TODO don't know what exactly should type of exception be to allow
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
    @DisplayName("configuration-describe-brokerLogger")
        // TODO should fail but doesn't
    void testConfigurationBrokerLogger(VertxTestContext context) {
        LOGGER.info("kafka-configs.sh --describe --entity-type brokerLogger <forbidden>, script representation test");
        LOGGER.info("getting entity description for brokerLogger: 0");
        admin.getConfigurationBrokerLogger("0")
                .onSuccess(response -> {
                    LOGGER.debug("this behaviour shouldn't be possible");
                    LOGGER.info("response size: {}", response.size());
                })
                .compose(r -> Future.failedFuture(" configuration-describe-brokerLogger shouldn't be permitted  "))
                .recover(throwable -> {
                    // TODO don't know what exactly should type of exception be to allow
                    if (throwable instanceof GroupNotEmptyException) {
                        LOGGER.info(throwable.getMessage());
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })

                .onComplete(context.succeedingThenComplete());
    }




    @Test
    @Order(3)
    @DisplayName("configuration-alter-users")
    void testConfigurationUsersAlter(VertxTestContext context) {
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
        LOGGER.info("kafka-consumer-groups.sh --list <permitted>, script representation test");
        LOGGER.info("listing all consumer groups");
        admin.listConsumerGroups()
                .onSuccess(response -> LOGGER.info("response size: {}", response.size()))
                .onComplete(context.succeedingThenComplete());
    }


    @Test
    @Order(4)
    @DisplayName("consumer-groups-groups-describe")
        // TODO should fail but doesn't
    void testConsumerGroupsDescribe(VertxTestContext context) {
        LOGGER.info("kafka-consumer-groups.sh --group --describe <permitted>, script representation test");
        LOGGER.info("describing specific consumer group");
        admin.describeConsumerGroups()
                .onSuccess(response -> LOGGER.info("response size: {}", response.size()))
                .onComplete(context.succeedingThenComplete());
    }


    @Test
    @Order(4)
    @DisplayName("consumer-groups-delete")
        // TODO should fail but doesn't
    void testConsumerGroupsDelete(VertxTestContext context) {
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
        LOGGER.info("kafka-consumer-groups.sh --all-groups --reset-offsets --execute --all-groups --all-topics  <permitted>, script representation test");
        admin.resetOffsets()
                .onSuccess(__ -> LOGGER.info("offset successfully reset"))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    @DisplayName("consumer-groups--delete-offsets")
    void testConsumerGroupsDeleteOffset(VertxTestContext context) {
        LOGGER.info("kafka-consumer-groups.sh --delete-offsets  <permitted>, script representation test");
        admin.deleteOffset()
                .onSuccess(__ -> LOGGER.info("offset successfully deleted"))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(4)
    @DisplayName("delete-records-offset-json-file")
    void testDeleteRecord(VertxTestContext context) {
        LOGGER.info("kafka-delete-records.sh --offset-json-file <permitted>, script representation test");
        admin.deleteRecords()
                .onSuccess(__ -> LOGGER.info("record successfully deleted"))
                .onComplete(context.succeedingThenComplete());
    }

        // TODO is ElectionNotNeededException enough, (same message in case of multiple replicas and partitions)
    @Test
    @Order(4)
    @DisplayName("leader-election--election-type UNCLEAN")
    void testLeaderElectionUnclean(VertxTestContext context) {
        LOGGER.info("kafka-leader-election.sh <forbidden>, script representation test");
        admin.electLeader(ElectionType.UNCLEAN)
                .compose(r -> Future.failedFuture(" Election is not needed "))
                .recover(throwable -> {
                    if (throwable instanceof ElectionNotNeededException) {
                        LOGGER.info(throwable.getMessage());
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }

    // TODO, should fail but doesn't
    @Test
    @Order(4)
    @DisplayName("log-dirs-describe")
    void testLogDirsDesribe(VertxTestContext context) {
        LOGGER.info("kafka-log-dirs.sh --describe <forbidden>, script representation test");
        admin.logDirs()
                .compose(r -> Future.failedFuture(" Attempt to read log directory on broker should not be permitted"))
                .recover(throwable -> {
                    // TODO don't know what exactly should type of exception be to allow
                    if (throwable instanceof ElectionNotNeededException) {
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }

    // TODO is ElectionNotNeededException enough, (same message in case of multiple replicas and partitions)
    @Test
    @Order(4)
    @DisplayName("preferred-replica-election")
    void testLeaderElectionPredered(VertxTestContext context) {
        LOGGER.info("kafka-preferred-replica-election.sh <forbidden>, script representation test");
        admin.electLeader(ElectionType.PREFERRED)
                .compose(r -> Future.failedFuture(" Election is not needed "))
                .recover(throwable -> {
                    if (throwable instanceof ElectionNotNeededException) {
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
    //TODO response is null, but still sucess responded
    void testReassignPartitions(VertxTestContext context) {
        LOGGER.info("kafka-reassign-partitions.sh <forbidden>, script representation test");
        admin.reassignPartitions()
                .compose(r -> Future.failedFuture(" reassigning of partitions shouldn't be possible"))
                .recover(throwable -> {
                    if (throwable instanceof ElectionNotNeededException) {
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
    //TODO in both tests is DelegationTokenDisabledException enoguh (delegation token feature is not enabled)
    void testCreateDelegationToken(VertxTestContext context) {
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