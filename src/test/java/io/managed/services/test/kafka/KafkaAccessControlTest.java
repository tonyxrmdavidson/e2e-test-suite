package io.managed.services.test.kafka;

import com.openshift.cloud.api.kas.auth.models.AclBinding;
import com.openshift.cloud.api.kas.auth.models.AclOperation;
import com.openshift.cloud.api.kas.auth.models.AclPatternType;
import com.openshift.cloud.api.kas.auth.models.AclPermissionType;
import com.openshift.cloud.api.kas.auth.models.AclResourceType;
import com.openshift.cloud.api.kas.auth.models.AclResourceTypeFilter;
import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.TopicSettings;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.exception.ApiForbiddenException;
import io.managed.services.test.client.exception.ApiNotFoundException;
import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.kafka.KafkaAuthMethod;
import io.managed.services.test.client.kafka.KafkaConsumerClient;
import io.managed.services.test.client.kafka.KafkaProducerClient;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.oauth.KeycloakLoginSession;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import lombok.SneakyThrows;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.DelegationTokenDisabledException;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;
import org.apache.kafka.common.errors.GroupAuthorizationException;

import java.util.List;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopic;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

/**
 * <p>
 * <b>Requires:</b>
 * <ul>
 *     <li> PRIMARY_USERNAME
 *     <li> PRIMARY_PASSWORD
 *     <li> SECONDARY_USERNAME
 *     <li> SECONDARY_PASSWORD
 *     <li> ALIEN_USERNAME
 *     <li> ALIEN_PASSWORD
 * </ul>
 */
public class KafkaAccessControlTest extends TestBase {
    // TODO At least all described tests in the document are automated
    //TODO KafkaMgmtAPIPermissionTest (migrate completely)
    //TODO KafkaAdminPermissionTest (migrate completely)
    //TODO KafkaInstanceAPITest (migrate all permission tests)

    private static final Logger LOGGER = LogManager.getLogger(KafkaAccessControlTest.class);

    //private static final String KAFKA_INSTANCE_NAME = "mk-e2e-ac-" + Environment.LAUNCH_KEY;
    // TODO change to previous name
    private static final String KAFKA_INSTANCE_NAME = "mk-e2e-up-" + Environment.LAUNCH_KEY;
    private static final String PRIMARY_SERVICE_ACCOUNT_NAME = "mk-e2e-ac-primary-sa-" + Environment.LAUNCH_KEY;
    private static final String SERVICE_ACCOUNT_NAME = PRIMARY_SERVICE_ACCOUNT_NAME;
    private static final String DEFAULT_SERVICE_ACCOUNT_NAME = "mk-e2e-ac-default-sa";
    private static final String SECONDARY_SERVICE_ACCOUNT_NAME = "mk-e2e-ac-secondary-sa-" + Environment.LAUNCH_KEY;
    private static final String ALIEN_SERVICE_ACCOUNT_NAME = "mk-e2e-ac-alien-sa-" + Environment.LAUNCH_KEY;

    private static final String TOPIC_NAME_EXISTING_TOPIC = "existing-topic-name";

    private ApplicationServicesApi primaryAPI;
    private ApplicationServicesApi secondaryAPI;
    private ApplicationServicesApi alienAPI;
    private ApplicationServicesApi adminAPI;

    private ServiceAccount primaryServiceAccount;
    private ServiceAccount secondaryServiceAccount;
    // service account that undergoes ACL permission changes. Named "default" because all exclusively its ACLs are cleaned before each test
    private ServiceAccount defaultServiceAccount;
    private ServiceAccount alienServiceAccount;

    private KafkaRequest kafka;
    private KafkaInstanceApi kafkaInstanceAPIPrimaryUser;
    private KafkaInstanceApi kafkaInstanceAPISecondaryUser;
    private KafkaInstanceApi kafkaInstanceAPIAdminUser;

    private KafkaAdmin primaryAdmin;
    private KafkaAdmin secondaryAdmin;
    private KafkaAdmin defaultAdmin;

    private List<AclBinding> defaultPermissionsList ;

    @BeforeClass
    @SneakyThrows
    public void bootstrap() {

        assertNotNull(Environment.ADMIN_USERNAME, "the ADMIN_USERNAME env is null");
        assertNotNull(Environment.ADMIN_PASSWORD, "the ADMIN_PASSWORD env is null");
        assertNotNull(Environment.PRIMARY_USERNAME, "the PRIMARY_USERNAME env is null");
        assertNotNull(Environment.PRIMARY_PASSWORD, "the PRIMARY_PASSWORD env is null");
        assertNotNull(Environment.SECONDARY_USERNAME, "the SECONDARY_USERNAME env is null");
        assertNotNull(Environment.SECONDARY_PASSWORD, "the SECONDARY_PASSWORD env is null");
        assertNotNull(Environment.ALIEN_USERNAME, "the ALIEN_USERNAME env is null");
        assertNotNull(Environment.ALIEN_PASSWORD, "the ALIEN_PASSWORD env is null");

        primaryAPI = ApplicationServicesApi.applicationServicesApi(
                Environment.PRIMARY_USERNAME,
                Environment.PRIMARY_PASSWORD);

        secondaryAPI = ApplicationServicesApi.applicationServicesApi(
                Environment.SECONDARY_USERNAME,
                Environment.SECONDARY_PASSWORD);

        alienAPI = ApplicationServicesApi.applicationServicesApi(
                Environment.ALIEN_USERNAME,
                Environment.ALIEN_PASSWORD);

        adminAPI = ApplicationServicesApi.applicationServicesApi(
                Environment.ADMIN_USERNAME,
                Environment.ADMIN_PASSWORD);

        LOGGER.info("create kafka instance '{}'", KAFKA_INSTANCE_NAME);
        kafka = KafkaMgmtApiUtils.applyKafkaInstance(primaryAPI.kafkaMgmt(), KAFKA_INSTANCE_NAME);

        secondaryServiceAccount =
                SecurityMgmtAPIUtils.applyServiceAccount(secondaryAPI.securityMgmt(), SECONDARY_SERVICE_ACCOUNT_NAME);
        primaryServiceAccount =
                SecurityMgmtAPIUtils.applyServiceAccount(primaryAPI.securityMgmt(), SERVICE_ACCOUNT_NAME);
        defaultServiceAccount =
                SecurityMgmtAPIUtils.applyServiceAccount(primaryAPI.securityMgmt(), DEFAULT_SERVICE_ACCOUNT_NAME);

        // create the kafka admin
        primaryAdmin = new KafkaAdmin(
                kafka.getBootstrapServerHost(),
                primaryServiceAccount.getClientId(),
                primaryServiceAccount.getClientSecret());

        // create the kafka admin
        secondaryAdmin = new KafkaAdmin(
                kafka.getBootstrapServerHost(),
                secondaryServiceAccount.getClientId(),
                secondaryServiceAccount.getClientSecret());

        // create default kafka admin
        defaultAdmin = new KafkaAdmin(
                kafka.getBootstrapServerHost(),
                defaultServiceAccount.getClientId(),
                defaultServiceAccount.getClientSecret());
        LOGGER.info("kafka admin api initialized for instance: {}", kafka.getBootstrapServerHost());

        // login to get access to Kafka Instance API for primary user.
        var auth = new KeycloakLoginSession(Environment.PRIMARY_USERNAME, Environment.PRIMARY_PASSWORD);
        var kafka = KafkaMgmtApiUtils.applyKafkaInstance(primaryAPI.kafkaMgmt(), KAFKA_INSTANCE_NAME);
        var masUser = bwait(auth.loginToOpenshiftIdentity());
        kafkaInstanceAPIPrimaryUser = KafkaInstanceApiUtils.kafkaInstanceApi(kafka, masUser);

        // login to get access to Kafka Instance API for secondary user
        kafkaInstanceAPISecondaryUser = bwait(KafkaInstanceApiUtils.kafkaInstanceApi(kafka,
                Environment.SECONDARY_USERNAME,
                Environment.SECONDARY_PASSWORD));

        // login to get access to Kafka Instance API for admin user
        kafkaInstanceAPIAdminUser = bwait(KafkaInstanceApiUtils.kafkaInstanceApi(kafka,
                Environment.ADMIN_USERNAME,
                Environment.ADMIN_PASSWORD));

        // get default ACLs for Kafka Instance
        defaultPermissionsList = KafkaInstanceApiUtils.getDefaultACLs(kafkaInstanceAPIPrimaryUser);

        // create topic that is needed to perform some permission test (e.g., messages consumption)
        KafkaInstanceApiUtils.applyTopic(kafkaInstanceAPIPrimaryUser, TOPIC_NAME_EXISTING_TOPIC);
    }

    @AfterClass(alwaysRun = true)
    @SneakyThrows
    public void teardown() {

        if (primaryAdmin != null) {
            // close KafkaAdmin
            primaryAdmin.close();
        }

        //Clear all but default ACLs....
        try {
            KafkaInstanceApiUtils.removeAllButDefaultACLs(kafkaInstanceAPIPrimaryUser, defaultPermissionsList);
        } catch (Throwable t){
            LOGGER.error("clean extra ACLs error: ", t);
        }


        assumeTeardown();

        try {
            KafkaMgmtApiUtils.cleanKafkaInstance(adminAPI.kafkaMgmt(), KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean kafka error: ", t);
        }
        try {
            SecurityMgmtAPIUtils.cleanServiceAccount(primaryAPI.securityMgmt(), PRIMARY_SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean main (primary) service account error: ", t);
        }

        try {
            SecurityMgmtAPIUtils.cleanServiceAccount(secondaryAPI.securityMgmt(), SECONDARY_SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean secondary service account error: ", t);
        }

        try {
            SecurityMgmtAPIUtils.cleanServiceAccount(alienAPI.securityMgmt(), ALIEN_SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean alien service account error: ", t);
        }

    }

    @Ignore
    @Test
    @SneakyThrows
    public void testSecondaryUserCanReadTheKafkaInstance() {

        // Get kafka instance list by another user with same org
        LOGGER.info("fetch list of kafka instance from the secondary user in the same org");
        var kafkas = secondaryAPI.kafkaMgmt().getKafkas(null, null, null, null);

        LOGGER.debug(kafkas);

        var o = kafkas.getItems().stream()
                .filter(k -> KAFKA_INSTANCE_NAME.equals(k.getName()))
                .findAny();
        assertTrue(o.isPresent());
    }
    @Ignore
    @Test
    @SneakyThrows
    public void testAlienUserCanNotReadTheKafkaInstance() {

        // Get list of kafka Instance in org 1 and test it should be there
        LOGGER.info("fetch list of kafka instance from the alin user in a different org");
        var kafkas = alienAPI.kafkaMgmt().getKafkas(null, null, null, null);

        LOGGER.debug(kafkas);

        var o = kafkas.getItems().stream()
                .filter(k -> KAFKA_INSTANCE_NAME.equals(k.getName()))
                .findAny();
        assertTrue(o.isEmpty());
    }
    @Ignore
    // always denied operations
    @Test
    public void testForbiddenToCreateDelegationToken() {

        LOGGER.info("kafka-delegation-tokens.sh create <forbidden>, script representation test");
        assertThrows(DelegationTokenDisabledException.class, () -> primaryAdmin.createDelegationToken());
    }
    @Ignore
    @Test
    public void testForbiddenToDescribeDelegationToken() {

        LOGGER.info("kafka-delegation-tokens.sh describe <forbidden>, script representation test");
        assertThrows(DelegationTokenDisabledException.class, () -> primaryAdmin.describeDelegationToken());
    }
    @Ignore
    @Test
    public void testForbiddenToUncleanLeaderElection() {

        LOGGER.info("kafka-leader-election.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> primaryAdmin.electLeader(ElectionType.UNCLEAN, TOPIC_NAME_EXISTING_TOPIC));
    }
    @Ignore
    @Test
    public void testForbiddenToDescribeLogDirs() {

        LOGGER.info("kafka-log-dirs.sh --describe <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> primaryAdmin.logDirs());
    }
    @Ignore
    @Test
    public void testForbiddenToAlterPreferredReplicaElection() {

        LOGGER.info("kafka-preferred-replica-election.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> primaryAdmin.electLeader(ElectionType.PREFERRED, TOPIC_NAME_EXISTING_TOPIC));
    }
    @Ignore
    @Test
    public void testForbiddenToReassignPartitions() {

        LOGGER.info("kafka-reassign-partitions.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> primaryAdmin.reassignPartitions(TOPIC_NAME_EXISTING_TOPIC));
    }

    // default permission of SA
    @Ignore
    @Test
    @SneakyThrows
    public void testACLsDefaultServiceAccountCanListTopic() {

        // removal of possibly existing additional ACLs
        KafkaInstanceApiUtils.removeAllButDefaultACLs(kafkaInstanceAPIPrimaryUser, defaultPermissionsList);

        LOGGER.info("Test default service account ability to list topics");
        defaultAdmin.listTopics();
    }
    @Ignore
    @Test
    @SneakyThrows
    public void testACLsDefaultServiceAccountCannotProduceAndConsumeMessages() {

        // removal of possibly existing additional ACLs
        KafkaInstanceApiUtils.removeAllButDefaultACLs(kafkaInstanceAPIPrimaryUser, defaultPermissionsList);

        LOGGER.info("Test default service account inability to produce and consume data from topic {}", TOPIC_NAME_EXISTING_TOPIC);
        assertThrows(GroupAuthorizationException.class, () -> bwait(testTopic(
                Vertx.vertx(),
                kafka.getBootstrapServerHost(),
                defaultServiceAccount.getClientId(),
                defaultServiceAccount.getClientSecret(),
                TOPIC_NAME_EXISTING_TOPIC,
                1000,
                10,
                100,
                KafkaAuthMethod.PLAIN)));
    }
    @Ignore
    @Test
    @SneakyThrows
    public void testACLsDefaultServiceAccountCannotCreateACLs() {

        // removal of possibly existing additional ACLs
        KafkaInstanceApiUtils.removeAllButDefaultACLs(kafkaInstanceAPIPrimaryUser, defaultPermissionsList);
        LOGGER.info("Test default service account inability to create ACL");
        assertThrows(ClusterAuthorizationException.class, () -> primaryAdmin.addAclResource(ResourceType.TOPIC));
    }
    @Ignore
    @Test
    @SneakyThrows
    public void testACLsAllowAllTopicServiceAccountCanCreateTopic() {

        LOGGER.info("Test ability of default service account with additional ACLs to create topic");

        // clean and add ACLs on all resources for default account
        KafkaInstanceApiUtils.removeAllButDefaultACLs(kafkaInstanceAPIPrimaryUser, defaultPermissionsList);
        KafkaInstanceApiUtils.applyAllowAllACLsOnResources(kafkaInstanceAPIPrimaryUser, defaultServiceAccount,
                List.of(AclResourceType.TOPIC)
        );

        final var topicName = "secondary-test-topic-x11";
        LOGGER.info("create kafka topic '{}'", topicName);

        defaultAdmin.createTopic(topicName);

        // teardown
        try{
            kafkaInstanceAPIPrimaryUser.deleteTopic(topicName);
        } catch (Exception e){
            LOGGER.error("error while deleting topic {}, {}",topicName, e.getMessage());
        }
    }
    @Ignore
    @Test
    @SneakyThrows
    public void testACLsServiceAccountCanProduceAndConsumeMessages() {

        LOGGER.info("Test ability of default service account with additional ACLs to create topic");

        // removal of possibly existing additional ACLs
        KafkaInstanceApiUtils.removeAllButDefaultACLs(kafkaInstanceAPIPrimaryUser, defaultPermissionsList);
        KafkaInstanceApiUtils.applyAllowAllACLsOnResources(kafkaInstanceAPIPrimaryUser, defaultServiceAccount,
                List.of(AclResourceType.TOPIC, AclResourceType.GROUP, AclResourceType.TRANSACTIONAL_ID)
        );

        LOGGER.info("Test default service account ability to produce and consume data from topic {} after ACLs applied", TOPIC_NAME_EXISTING_TOPIC);
        bwait(testTopic(
                Vertx.vertx(),
                kafka.getBootstrapServerHost(),
                defaultServiceAccount.getClientId(),
                defaultServiceAccount.getClientSecret(),
                TOPIC_NAME_EXISTING_TOPIC,
                1000,
                10,
                100,
                KafkaAuthMethod.PLAIN));

    }
    @Ignore
    @Test
    @SneakyThrows
    public void testACLsServiceAccountCanListConsumerGroups() {

        LOGGER.info("Test ability of default service account with additional ACLs to list consumer groups");
        // removal of possibly existing additional ACLs
        KafkaInstanceApiUtils.removeAllButDefaultACLs(kafkaInstanceAPIPrimaryUser, defaultPermissionsList);
        KafkaInstanceApiUtils.applyAllowAllACLsOnResources(kafkaInstanceAPIPrimaryUser, defaultServiceAccount,
                List.of(AclResourceType.TOPIC, AclResourceType.GROUP, AclResourceType.TRANSACTIONAL_ID)
        );

        defaultAdmin.listConsumerGroups();

    }
    @Ignore
    @Test
    @SneakyThrows
    public void testACLsServiceAccountCanDeleteConsumerGroups() {

        LOGGER.info("Test ability of default service account with additional ACLs to delete consumer groups");
        final String groupId = "cg-3";

        // add ACLs on all resources for default account
        KafkaInstanceApiUtils.removeAllButDefaultACLs(kafkaInstanceAPIPrimaryUser, defaultPermissionsList);
        KafkaInstanceApiUtils.applyAllowAllACLsOnResources(kafkaInstanceAPIPrimaryUser, defaultServiceAccount,
                List.of(AclResourceType.TOPIC, AclResourceType.GROUP, AclResourceType.TRANSACTIONAL_ID)
        );

        try (var consumerClient = new KafkaConsumerClient<>(
                Vertx.vertx(),
                kafka.getBootstrapServerHost(),
                defaultServiceAccount.getClientId(), defaultServiceAccount.getClientSecret(),
                KafkaAuthMethod.OAUTH,
                groupId,
                "latest",
                StringDeserializer.class,
                StringDeserializer.class)) {

            bwait(consumerClient.receiveAsync(TOPIC_NAME_EXISTING_TOPIC, 0));
        }

        defaultAdmin.deleteConsumerGroups(groupId);
    }


    @Ignore
    @Test
    @SneakyThrows
    public void testACLsDenyTopicReadConnectedConsumerCannotReadMoreMessages() {

        LOGGER.info("Test ability of default service account with additional ACLs to list consumer groups");

        // add ACLs on all resources for default account
        KafkaInstanceApiUtils.removeAllButDefaultACLs(kafkaInstanceAPIPrimaryUser, defaultPermissionsList);
        KafkaInstanceApiUtils.applyAllowAllACLsOnResources(kafkaInstanceAPIPrimaryUser, defaultServiceAccount,
                List.of(AclResourceType.TOPIC, AclResourceType.GROUP, AclResourceType.TRANSACTIONAL_ID)
        );

        // create producer
        var producer = new KafkaProducerClient<String, String>(
            Vertx.vertx(),
            kafka.getBootstrapServerHost(),
            defaultServiceAccount.getClientId(), defaultServiceAccount.getClientSecret(),
            KafkaAuthMethod.OAUTH,
            StringSerializer.class,
            StringSerializer.class
            );

        // create consumer
        var consumerClient = new KafkaConsumerClient<>(
                Vertx.vertx(),
                kafka.getBootstrapServerHost(),
                defaultServiceAccount.getClientId(), defaultServiceAccount.getClientSecret(),
                KafkaAuthMethod.OAUTH,
                "groupId",
                "earliest",
                StringDeserializer.class,
                StringDeserializer.class);

        // produce message
        LOGGER.info("Producer produce single message");
        bwait(producer.send(KafkaProducerRecord.create(TOPIC_NAME_EXISTING_TOPIC, "message 1")));

        // consume messages
        LOGGER.info("Consumer reads single message");
        bwait(consumerClient.receiveAsync(TOPIC_NAME_EXISTING_TOPIC, 1));

        // deny rights
        LOGGER.info("new ACL that deny right to read Topics for tested service account is to be applied");
        var principal = KafkaInstanceApiUtils.toPrincipal(defaultServiceAccount.getClientId());
        var acl = new AclBinding()
                .principal(principal)
                .resourceType(AclResourceType.TOPIC)
                .patternType(AclPatternType.LITERAL)
                .resourceName("*")
                .permission(AclPermissionType.DENY)
                .operation(AclOperation.READ);
        kafkaInstanceAPIPrimaryUser.createAcl(acl);

        LOGGER.info("ACL is applied");

        // produce one more message
        LOGGER.info("Producer produce another message (after ACL to DENY TOPIC READ is applied)");
        bwait(producer.send(KafkaProducerRecord.create(TOPIC_NAME_EXISTING_TOPIC, "message 2")));

        // fail while consuming message
        LOGGER.info("Consumer wants to  read another message (after ACL to DENY TOPIC READ is applied)");

        assertThrows(AuthorizationException.class, () -> bwait(consumerClient.tryConsumingMessages(1)));

        //close consumer
        consumerClient.close();
    }
    @Ignore
    @Test
    @SneakyThrows
    public void testACLsDenyTopicDeletionWithPrefix() {

        LOGGER.info("Test ACL ability to deny service account to delete topic with prefix");

        final String prefix = "prefix-1-";
        final String topicName = "topic-delete-name";
        final String topicNamePrefixed = prefix + topicName;

        // add ACLs on all resources for default account
        KafkaInstanceApiUtils.removeAllButDefaultACLs(kafkaInstanceAPIPrimaryUser, defaultPermissionsList);
        KafkaInstanceApiUtils.applyAllowAllACLsOnResources(kafkaInstanceAPIPrimaryUser, defaultServiceAccount,
                List.of(AclResourceType.TOPIC)
        );
        // add ACL to Deny deletion of topic with prefix for all of users
        var acl = new AclBinding()
                .principal("User:*")
                .resourceType(AclResourceType.TOPIC)
                .patternType(AclPatternType.PREFIXED)
                .resourceName(prefix)
                .permission(AclPermissionType.DENY)
                .operation(AclOperation.DELETE);
        kafkaInstanceAPIPrimaryUser.createAcl(acl);

        LOGGER.info("create topic: {}, that does not match prefix: {}",topicName, prefix);
        defaultAdmin.createTopic(topicName, 1, (short) 3);

        LOGGER.info("create topic: {}, that matches prefix: {}", topicNamePrefixed, prefix);
        defaultAdmin.createTopic(topicNamePrefixed, 1, (short) 3);

        LOGGER.info("delete topic: {}, that does not match prefix: {}", topicName, prefix);
        defaultAdmin.deleteTopic(topicName);

        LOGGER.info("fail to delete topic: {}, that matches prefix: {}", topicNamePrefixed, prefix);
        assertThrows(AuthorizationException.class, () -> defaultAdmin.deleteTopic(topicNamePrefixed));

        // clean up
        LOGGER.info("fail to delete topic: {}, that matches prefix: {}", topicNamePrefixed, prefix);
        try{
            kafkaInstanceAPIPrimaryUser.deleteTopic(topicNamePrefixed);
        } catch (Exception e){
            LOGGER.error("error while deleting prefixed topic {}, {}",topicNamePrefixed, e.getMessage());
        }
    }
    @Ignore
    @Test( dependsOnMethods = "testACLsDenyTopicReadConnectedConsumerCannotReadMoreMessages")
    @SneakyThrows
    public void testACLsDenyTopicDescribeConsumerGroupAll() {
        // dependency on other test is here to decrease number of steps required to set up this test (it needs at least 1 topic and 1 consumer group)
        LOGGER.info("Test ACL ability to deny rights to work with consumer group (i.e., read )");

        // add ACLs on all resources for default account
        KafkaInstanceApiUtils.removeAllButDefaultACLs(kafkaInstanceAPIPrimaryUser, defaultPermissionsList);
        KafkaInstanceApiUtils.applyAllowAllACLsOnResources(kafkaInstanceAPIPrimaryUser, defaultServiceAccount,
                List.of( AclResourceType.GROUP, AclResourceType.TOPIC)
        );

        // add ACL: Deny Topic Describe to all
        LOGGER.info("ACL: deny describe Topic to service account");
        var principal = KafkaInstanceApiUtils.toPrincipal(defaultServiceAccount.getClientId());
        var acl = new AclBinding()
                // for every user
                .principal(principal)
                .resourceType(AclResourceType.TOPIC)
                .patternType(AclPatternType.LITERAL)
                .resourceName("*")
                .permission(AclPermissionType.DENY)
                .operation(AclOperation.DESCRIBE);

        kafkaInstanceAPIPrimaryUser.createAcl(acl);

        // deny rights
        LOGGER.info("ACL: deny read Topic for default service account");

        acl = new AclBinding()
                .principal(principal)
                .resourceType(AclResourceType.TOPIC)
                .patternType(AclPatternType.LITERAL)
                .resourceName("*")
                .permission(AclPermissionType.DENY)
                .operation(AclOperation.READ);

        kafkaInstanceAPIPrimaryUser.createAcl(acl);

        LOGGER.info("ACL: deny all groups for default service account");
        acl = new AclBinding()
                .principal(principal)
                .resourceType(AclResourceType.GROUP)
                .patternType(AclPatternType.LITERAL)
                .resourceName("*")
                .permission(AclPermissionType.DENY)
                .operation(AclOperation.ALL);

        kafkaInstanceAPIPrimaryUser.createAcl(acl);

        LOGGER.info("try to list topics");
        assertTrue(defaultAdmin.listTopics().isEmpty(),"Unauthorized SA should request empty list of topics");

        LOGGER.info("try to list consumer groups");
        assertTrue(defaultAdmin.listConsumerGroups().isEmpty(),"Unauthorized SA should request empty list of consumer groups");

        LOGGER.info("try to delete consumer groups");
        assertThrows(AuthorizationException.class, () -> defaultAdmin.deleteConsumerGroups("something"));
    }
    @Ignore
    @Test
    @SneakyThrows
    public void testDefaultSecondaryUserCanListACLs() {

        LOGGER.info("Test ACL default setting, secondary user can list ACLs");

        // clean ACLs
        KafkaInstanceApiUtils.removeAllButDefaultACLs(kafkaInstanceAPIPrimaryUser, defaultPermissionsList);

        kafkaInstanceAPISecondaryUser.getAcls(null,null,null,null,null,null, null, null, null,null);
    }
    @Ignore
    @Test
    @SneakyThrows
    public void testDefaultSecondaryUserCannotDeleteACLs() {

        LOGGER.info("Test ACL default setting, secondary user cannot delete ACLs");
        // clean ACLs
        KafkaInstanceApiUtils.removeAllButDefaultACLs(kafkaInstanceAPIPrimaryUser, defaultPermissionsList);

        assertThrows(ApiForbiddenException.class, () ->
                kafkaInstanceAPISecondaryUser.deleteAcls(null, null, null, null, null, null));
    }
    @Ignore
    @Test
    @SneakyThrows
    public void testACLsTopicsAllowAllForSecondaryUser() {

        LOGGER.info("Test default setting, secondary user cannot delete ACLs");
        final String topicName = "topic-name-acl-secondary-acc";
        // clean ACLs
        KafkaInstanceApiUtils.removeAllButDefaultACLs(kafkaInstanceAPIPrimaryUser, defaultPermissionsList);

        assertThrows(AuthorizationException.class, () -> defaultAdmin.deleteAclResource(ResourceType.ANY));

        // add ACL to Deny deletion of topic with prefix for all of users
        var acl = new AclBinding()
                .principal(KafkaInstanceApiUtils.toPrincipal(Environment.SECONDARY_USERNAME))
                .resourceType(AclResourceType.TOPIC)
                .patternType(AclPatternType.LITERAL)
                .resourceName("*")
                .permission(AclPermissionType.ALLOW)
                .operation(AclOperation.ALL);
        kafkaInstanceAPIPrimaryUser.createAcl(acl);

        // topic create
        LOGGER.info("Secondary user creates topic {}", topicName);
        var payload = new NewTopicInput()
                .name(topicName)
                .settings(new TopicSettings().numPartitions(1));

        kafkaInstanceAPISecondaryUser.createTopic(payload);

        // topic delete
        LOGGER.info("Secondary user deletes topic {}", topicName);
        kafkaInstanceAPISecondaryUser.deleteTopic(topicName);
    }
    @Ignore
    @Test
    @SneakyThrows
    public void testDefaultAdminUserCannotCreateACLs() {

        LOGGER.info("Test ACL default setting, secondary user cannot delete ACLs");
        // clean ACLs
        KafkaInstanceApiUtils.removeAllButDefaultACLs(kafkaInstanceAPIPrimaryUser, defaultPermissionsList);

        // add ACL to Deny deletion of topic with prefix for all of users
        var acl = new AclBinding()
                .principal(KafkaInstanceApiUtils.toPrincipal(Environment.SECONDARY_USERNAME))
                .resourceType(AclResourceType.TOPIC)
                .patternType(AclPatternType.LITERAL)
                .resourceName("xxxx")
                .permission(AclPermissionType.ALLOW)
                .operation(AclOperation.ALL);

        assertThrows(ApiForbiddenException.class, () ->  kafkaInstanceAPIAdminUser.createAcl(acl));
    }
    @Ignore
    @Test
    @SneakyThrows
    public void testDefaultAdminUserCannotDeleteACLs() {

        LOGGER.info("Test ACL default setting, secondary user cannot delete ACLs");
        // clean ACLs
        KafkaInstanceApiUtils.removeAllButDefaultACLs(kafkaInstanceAPIPrimaryUser, defaultPermissionsList);

        assertThrows(ApiForbiddenException.class, () -> kafkaInstanceAPIAdminUser.deleteAcls(
            AclResourceTypeFilter.TOPIC, "xx", null, null, null, null));
    }
    @Ignore
    @SneakyThrows
    @Test (priority = 1)
    public void testAdminUserCanChangeTheKafkaInstanceOwner() {

        LOGGER.info("Test switch owner (admin) of kafka instance from primary user to secondary");
        // clean ACLs
        KafkaInstanceApiUtils.removeAllButDefaultACLs(kafkaInstanceAPIPrimaryUser, defaultPermissionsList);

        var authorizationTopicName = "authorization-topic-name";

        // verify that user who is not the owner of instance can not perform operation (e.g., create topic)
        assertThrows(
                ApiForbiddenException.class,
                () -> kafkaInstanceAPISecondaryUser.deleteTopic(authorizationTopicName)
        );

        LOGGER.info("change of owner of instance");
        KafkaMgmtApiUtils.changeKafkaInstanceOwner(adminAPI.kafkaMgmt(), kafka, Environment.SECONDARY_USERNAME);
        // wait until owner is changed (waiting for Rollout on Brokers)
        KafkaMgmtApiUtils.waitUntilOwnerIsChanged(kafkaInstanceAPISecondaryUser);

        // try to perform operation (i.e., delete topic) without Authorization exception
        LOGGER.info("deletion of topic should now pass authorization phase");
        assertThrows(
                ApiNotFoundException.class,
                () -> kafkaInstanceAPISecondaryUser.deleteTopic(authorizationTopicName)
        );
    }

    @SneakyThrows
    //@Test (dependsOnMethods = "testAdminUserCanChangeTheKafkaInstanceOwner")
    @Test
    public void testNewInstanceOwnerCanCreateACLs() {

        LOGGER.info("Test ACL default setting, new Instance owner (Secondary User) can create ACLs");
        // clean ACLs
        KafkaInstanceApiUtils.removeAllButDefaultACLs(kafkaInstanceAPISecondaryUser, defaultPermissionsList);

        // add ACL to Deny deletion of topic with prefix for all of users
        var acl = new AclBinding()
                .principal(KafkaInstanceApiUtils.toPrincipal(Environment.PRIMARY_USERNAME))
                .resourceType(AclResourceType.TOPIC)
                .patternType(AclPatternType.LITERAL)
                .resourceName("*")
                .permission(AclPermissionType.ALLOW)
                .operation(AclOperation.ALL);

        kafkaInstanceAPISecondaryUser.createAcl(acl);
    }

    @SneakyThrows
    //@Test (dependsOnMethods = "testAdminUserCanChangeTheKafkaInstanceOwner")
    @Test
    public void testNewInstanceOwnerCanDeleteACLs() {

        LOGGER.info("Test  new Instance owner (Secondary User) can create ACLs");
        // clean ACLs
        KafkaInstanceApiUtils.removeAllButDefaultACLs(kafkaInstanceAPISecondaryUser, defaultPermissionsList);

        kafkaInstanceAPISecondaryUser.deleteAcls(
            AclResourceTypeFilter.TOPIC, "xx", null, null, null, null);
    }

    @Test
    @SneakyThrows
    public void testOldInstanceOwnerCanListACLs() {

        LOGGER.info("Test old instance owner (primary user) can list ACLs");

        // clean ACLs
        KafkaInstanceApiUtils.removeAllButDefaultACLs(kafkaInstanceAPISecondaryUser, defaultPermissionsList);

        kafkaInstanceAPIPrimaryUser.getAcls(null,null,null,null,null,null, null, null, null,null);
    }

    @Test
    @SneakyThrows
    public void testOldInstanceOwnerCannotDeleteACLs() {

        LOGGER.info("Test old instance owner (primary user) can list ACLs");
        // clean ACLs
        KafkaInstanceApiUtils.removeAllButDefaultACLs(kafkaInstanceAPISecondaryUser, defaultPermissionsList);

        assertThrows(ApiForbiddenException.class, () ->
                kafkaInstanceAPIPrimaryUser.deleteAcls(null, null, null, null, null, null));
    }


    //@SneakyThrows
    //@Test (priority = 3)
    //// test is should be executed as last one.
    //public void testAdminUserCanDeleteTheKafkaInstance() {
    //    KafkaMgmtApiUtils.cleanKafkaInstance(adminAPI.kafkaMgmt(), KAFKA_INSTANCE_NAME);
    //}

}
