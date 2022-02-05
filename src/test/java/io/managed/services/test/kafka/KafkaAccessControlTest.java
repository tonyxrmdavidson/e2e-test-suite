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
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiAccessUtils;
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
    //TODO KafkaMgmtAPIPermissionTest (migrate completely)
    //TODO KafkaAdminPermissionTest (migrate completely)
    //TODO KafkaInstanceAPITest (migrate all permission tests)

    private static final Logger LOGGER = LogManager.getLogger(KafkaAccessControlTest.class);

    private static final String KAFKA_INSTANCE_NAME = "mk-e2e-ac-" + Environment.LAUNCH_KEY;
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

    private ServiceAccount serviceAccount;

    private KafkaRequest kafka;
    private KafkaInstanceApi kafkaInstanceAPIPrimaryUser;
    private KafkaInstanceApi kafkaInstanceAPISecondaryUser;
    private KafkaInstanceApi kafkaInstanceAPIAdminUser;

    private KafkaAdmin apacheKafkaAdmin;

    private List<AclBinding> defaultPermissionsList;

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

        serviceAccount =
                SecurityMgmtAPIUtils.applyServiceAccount(primaryAPI.securityMgmt(), SERVICE_ACCOUNT_NAME);

        // create the kafka admin (longer name is used to distinguish easily between several admins and kafka APIs)
        apacheKafkaAdmin = new KafkaAdmin(
                kafka.getBootstrapServerHost(),
                serviceAccount.getClientId(),
                serviceAccount.getClientSecret());

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
        defaultPermissionsList = KafkaInstanceApiAccessUtils.getDefaultACLs(kafkaInstanceAPIPrimaryUser);

        // create topic that is needed to perform some permission test (e.g., messages consumption)
        KafkaInstanceApiUtils.applyTopic(kafkaInstanceAPIPrimaryUser, TOPIC_NAME_EXISTING_TOPIC);
    }

    @AfterClass(alwaysRun = true)
    @SneakyThrows
    public void teardown() {

        if (apacheKafkaAdmin != null) {
            // close KafkaAdmin
            apacheKafkaAdmin.close();
        }

        //Clear all but default ACLs (i.e., those that instance had at moment of creation/application)
        try {
            KafkaInstanceApiAccessUtils.removeAllButDefaultACLs(kafkaInstanceAPIPrimaryUser, defaultPermissionsList);
        } catch (Throwable t) {
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

    // always denied operations
    @Test
    public void testAlwaysForbiddenToCreateDelegationToken() {

        LOGGER.info("kafka-delegation-tokens.sh create <forbidden>, script representation test");
        assertThrows(DelegationTokenDisabledException.class, () -> apacheKafkaAdmin.createDelegationToken());
    }

    @Test
    public void testAlwaysForbiddenToDescribeDelegationToken() {

        LOGGER.info("kafka-delegation-tokens.sh describe <forbidden>, script representation test");
        assertThrows(DelegationTokenDisabledException.class, () -> apacheKafkaAdmin.describeDelegationToken());
    }

    @Test
    public void testAlwaysForbiddenToUncleanLeaderElection() {

        LOGGER.info("kafka-leader-election.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> apacheKafkaAdmin.electLeader(ElectionType.UNCLEAN, TOPIC_NAME_EXISTING_TOPIC));
    }

    @Test
    public void testAlwaysForbiddenToDescribeLogDirs() {

        LOGGER.info("kafka-log-dirs.sh --describe <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> apacheKafkaAdmin.logDirs());
    }

    @Test
    public void testAlwaysForbiddenToAlterPreferredReplicaElection() {

        LOGGER.info("kafka-preferred-replica-election.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> apacheKafkaAdmin.electLeader(ElectionType.PREFERRED, TOPIC_NAME_EXISTING_TOPIC));
    }

    @Test
    public void testAlwaysForbiddenToReassignPartitions() {

        LOGGER.info("kafka-reassign-partitions.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> apacheKafkaAdmin.reassignPartitions(TOPIC_NAME_EXISTING_TOPIC));
    }

    // test default permission of SA

    @Test(priority = 1)
    @SneakyThrows
    public void testACLsDefaultPermissionsServiceAccountCanListTopic() {

        LOGGER.info("Test default service account ability to list topics");
        apacheKafkaAdmin.listTopics();
    }

    @Test(priority = 1)
    @SneakyThrows
    public void testACLsDefaultPermissionsServiceAccountCannotProduceAndConsumeMessages() {

        LOGGER.info("Test default service account inability to produce and consume data from topic {}", TOPIC_NAME_EXISTING_TOPIC);
        assertThrows(GroupAuthorizationException.class, () -> bwait(testTopic(
                Vertx.vertx(),
                kafka.getBootstrapServerHost(),
                serviceAccount.getClientId(),
                serviceAccount.getClientSecret(),
                TOPIC_NAME_EXISTING_TOPIC,
                1000,
                10,
                100,
                KafkaAuthMethod.PLAIN)));
    }

    @Test(priority = 1)
    @SneakyThrows
    public void testACLsDefaultPermissionsServiceAccountCannotCreateACLs() {

        LOGGER.info("Test default service account inability to create ACL");
        assertThrows(ClusterAuthorizationException.class, () -> apacheKafkaAdmin.addAclResource(ResourceType.TOPIC));
    }

    @Test(priority = 2)
    @SneakyThrows
    public void testACLsAllowAllTopicServiceAccountCanCreateTopic() {

        LOGGER.info("Test ability of default service account with additional ACLs to create topic");
        // assign appropriate ACLs
        KafkaInstanceApiAccessUtils.applyAllowAllACLsOnResources(kafkaInstanceAPIPrimaryUser, serviceAccount,
                List.of(AclResourceType.TOPIC, AclResourceType.GROUP, AclResourceType.TRANSACTIONAL_ID)
        );


        // creation of topic
        final var topicName = "secondary-test-topic-1";
        LOGGER.info("create kafka topic '{}'", topicName);
        apacheKafkaAdmin.createTopic(topicName);

        // teardown
        try {
            kafkaInstanceAPIPrimaryUser.deleteTopic(topicName);
        } catch (Exception e) {
            LOGGER.error("error while deleting topic {}, {}", topicName, e.getMessage());
        }
    }

    @Test(priority = 2, dependsOnMethods = "testACLsAllowAllTopicServiceAccountCanCreateTopic")
    @SneakyThrows
    public void testACLsServiceAccountCanProduceAndConsumeMessages() {

        LOGGER.info("Test ability of default service account with additional ACLs to create topic");

        LOGGER.info("Test default service account ability to produce and consume data from topic {} after ACLs applied", TOPIC_NAME_EXISTING_TOPIC);
        bwait(testTopic(
                Vertx.vertx(),
                kafka.getBootstrapServerHost(),
                serviceAccount.getClientId(),
                serviceAccount.getClientSecret(),
                TOPIC_NAME_EXISTING_TOPIC,
                1000,
                10,
                100,
                KafkaAuthMethod.PLAIN));

    }

    @Test(priority = 2, dependsOnMethods = "testACLsAllowAllTopicServiceAccountCanCreateTopic")
    @SneakyThrows
    public void testACLsServiceAccountCanListConsumerGroups() {

        LOGGER.info("Test ability of default service account with additional ACLs to list consumer groups");
        // removal of possibly existing additional ACLs
        KafkaInstanceApiAccessUtils.removeAllButDefaultACLs(kafkaInstanceAPIPrimaryUser, defaultPermissionsList);
        KafkaInstanceApiAccessUtils.applyAllowAllACLsOnResources(kafkaInstanceAPIPrimaryUser, serviceAccount,
                List.of(AclResourceType.TOPIC, AclResourceType.GROUP, AclResourceType.TRANSACTIONAL_ID)
        );

        apacheKafkaAdmin.listConsumerGroups();

    }

    @Test(priority = 2, dependsOnMethods = "testACLsAllowAllTopicServiceAccountCanCreateTopic")
    @SneakyThrows
    public void testACLsServiceAccountCanDeleteConsumerGroups() {

        LOGGER.info("Test ability of default service account with additional ACLs to delete consumer groups");
        final String groupId = "cg-3";

        try (var consumerClient = new KafkaConsumerClient<>(
                Vertx.vertx(),
                kafka.getBootstrapServerHost(),
                serviceAccount.getClientId(), serviceAccount.getClientSecret(),
                KafkaAuthMethod.OAUTH,
                groupId,
                "latest",
                StringDeserializer.class,
                StringDeserializer.class)) {

            bwait(consumerClient.receiveAsync(TOPIC_NAME_EXISTING_TOPIC, 0));
        }

        apacheKafkaAdmin.deleteConsumerGroups(groupId);
    }


    @Test(priority = 3, dependsOnMethods = "testACLsServiceAccountCanProduceAndConsumeMessages")
    @SneakyThrows
    public void testACLsDenyTopicReadConnectedConsumerCannotReadMoreMessages() {

        LOGGER.info("Test ability of default service account with additional ACLs to list consumer groups");

        // create producer
        var producer = new KafkaProducerClient<String, String>(
            Vertx.vertx(),
            kafka.getBootstrapServerHost(),
            serviceAccount.getClientId(), serviceAccount.getClientSecret(),
            KafkaAuthMethod.OAUTH,
            StringSerializer.class,
            StringSerializer.class
            );

        // new consumer that reads messages from beginning
        var consumerClient = new KafkaConsumerClient<>(
                Vertx.vertx(),
                kafka.getBootstrapServerHost(),
                serviceAccount.getClientId(), serviceAccount.getClientSecret(),
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

        // deny rights (deny topic read for service account)
        LOGGER.info("new ACL that deny right to read Topics for tested service account is to be applied");
        var principal = KafkaInstanceApiAccessUtils.toPrincipal(serviceAccount.getClientId());
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

    @Test(priority = 4, dependsOnMethods = "testACLsAllowAllTopicServiceAccountCanCreateTopic")
    @SneakyThrows
    public void testACLsDenyTopicDeletionWithPrefixToAllUsers() {

        LOGGER.info("Test ACL ability to deny service account to delete topic with prefix");

        final String prefix = "prefix-1-";
        final String topicName = "topic-delete-name";
        final String topicNamePrefixed = prefix + topicName;

        // add ACL to Deny deletion of topic with prefix for all of users
        var acl = new AclBinding()
                .principal("User:*")
                .resourceType(AclResourceType.TOPIC)
                .patternType(AclPatternType.PREFIXED)
                .resourceName(prefix)
                .permission(AclPermissionType.DENY)
                .operation(AclOperation.DELETE);
        kafkaInstanceAPIPrimaryUser.createAcl(acl);

        LOGGER.info("create topic: {}, that does not match prefix: {}", topicName, prefix);
        apacheKafkaAdmin.createTopic(topicName, 1, (short) 3);

        LOGGER.info("create topic: {}, that matches prefix: {}", topicNamePrefixed, prefix);
        apacheKafkaAdmin.createTopic(topicNamePrefixed, 1, (short) 3);

        LOGGER.info("delete topic: {}, that does not match prefix: {}", topicName, prefix);
        apacheKafkaAdmin.deleteTopic(topicName);

        LOGGER.info("fail to delete topic: {}, that matches prefix: {}", topicNamePrefixed, prefix);
        assertThrows(AuthorizationException.class, () -> apacheKafkaAdmin.deleteTopic(topicNamePrefixed));

        // clean up
        LOGGER.info("fail to delete topic: {}, that matches prefix: {}", topicNamePrefixed, prefix);
        try {
            kafkaInstanceAPIPrimaryUser.deleteTopic(topicNamePrefixed);
        } catch (Exception e) {
            LOGGER.error("error while deleting prefixed topic {}, {}", topicNamePrefixed, e.getMessage());
        }
    }

    @Test (priority = 5, dependsOnMethods = "testACLsAllowAllTopicServiceAccountCanCreateTopic")
    @SneakyThrows
    public void testACLsDenyTopicDescribeConsumerGroupAll() {
        // dependency on other test is here to decrease number of steps required to set up this test (it needs at least 1 topic and 1 consumer group)
        LOGGER.info("Test ACL ability to deny rights to work with consumer group (i.e., read )");

        // add ACLs on all resources for default account
        KafkaInstanceApiAccessUtils.removeAllButDefaultACLs(kafkaInstanceAPIPrimaryUser, defaultPermissionsList);
        KafkaInstanceApiAccessUtils.applyAllowAllACLsOnResources(kafkaInstanceAPIPrimaryUser, serviceAccount,
                List.of(AclResourceType.GROUP, AclResourceType.TOPIC)
        );

        // add ACL: Deny Topic Describe to all
        LOGGER.info("ACL: deny describe Topic to service account");
        var principal = KafkaInstanceApiAccessUtils.toPrincipal(serviceAccount.getClientId());
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
        assertTrue(apacheKafkaAdmin.listTopics().isEmpty(), "Unauthorized SA should request empty list of topics");

        LOGGER.info("try to list consumer groups");
        assertTrue(apacheKafkaAdmin.listConsumerGroups().isEmpty(), "Unauthorized SA should request empty list of consumer groups");

        LOGGER.info("try to delete consumer groups");
        assertThrows(AuthorizationException.class, () -> apacheKafkaAdmin.deleteConsumerGroups("something"));
    }

    @Test(priority = 1)
    @SneakyThrows
    public void testDefaultPermissionsSecondaryUserCanListACLs() {

        LOGGER.info("Test ACL default setting, secondary user can list ACLs");
        kafkaInstanceAPISecondaryUser.getAcls(null, null, null, null, null, null, null, null, null, null);
    }

    @Test(priority = 1)
    @SneakyThrows
    public void testDefaultSecondaryUserCannotDeleteACLs() {

        LOGGER.info("Test ACL default setting, secondary user cannot delete ACLs");
        assertThrows(ApiForbiddenException.class, () ->
                kafkaInstanceAPISecondaryUser.deleteAcls(null, null, null, null, null, null));
    }

    @Test(priority = 2)
    @SneakyThrows
    public void testACLsTopicsAllowAllForSecondaryUser() {

        LOGGER.info("Test default setting, secondary user cannot delete ACLs");
        final String topicName = "topic-name-acl-secondary-acc";

        // add ACL to Deny deletion of topic with prefix for all of users
        var acl = new AclBinding()
                .principal(KafkaInstanceApiAccessUtils.toPrincipal(Environment.SECONDARY_USERNAME))
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

    @Test(priority = 1)
    @SneakyThrows
    public void testDefaultPermissionsAdminUserCannotCreateACLs() {

        LOGGER.info("Test ACL default setting, secondary user cannot delete ACLs");

        // add ACL to Deny deletion of topic with prefix for all of users
        var acl = new AclBinding()
                .principal(KafkaInstanceApiAccessUtils.toPrincipal(Environment.SECONDARY_USERNAME))
                .resourceType(AclResourceType.TOPIC)
                .patternType(AclPatternType.LITERAL)
                .resourceName("xyz")
                .permission(AclPermissionType.ALLOW)
                .operation(AclOperation.ALL);

        assertThrows(ApiForbiddenException.class, () ->  kafkaInstanceAPIAdminUser.createAcl(acl));
    }

    @Test(priority = 1)
    @SneakyThrows
    public void testDefaultPermissionsAdminUserCannotDeleteACLs() {

        LOGGER.info("Test ACL default setting, secondary user cannot delete ACLs");
        assertThrows(ApiForbiddenException.class, () -> kafkaInstanceAPIAdminUser.deleteAcls(
            AclResourceTypeFilter.TOPIC, "xx", null, null, null, null));
    }
    //// TODO delete and change the other one to testAdminUserCanChangeTheKafkaInstanceOwner
    //@Test (priority = 10)
    //public void testAdminUserCanChangeTheKafkaInstanceOwner() {
    //
    //}

    //@Ignore
    @SneakyThrows
    @Test (priority = 10)
    public void testAdminUserCanChangeTheKafkaInstanceOwner() {

        LOGGER.info("Test switch owner (admin) of kafka instance from primary user to secondary");
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
    @Test (dependsOnMethods = "testAdminUserCanChangeTheKafkaInstanceOwner")
    public void testPermissionsNewInstanceOwnerCanCreateACLs() {

        LOGGER.info("Test ACL default setting, new Instance owner (Secondary User) can create ACLs");

        // add ACL to Deny deletion of topic with prefix for all of users
        var acl = new AclBinding()
                .principal(KafkaInstanceApiAccessUtils.toPrincipal(Environment.PRIMARY_USERNAME))
                .resourceType(AclResourceType.TOPIC)
                .patternType(AclPatternType.LITERAL)
                .resourceName("*")
                .permission(AclPermissionType.ALLOW)
                .operation(AclOperation.ALL);

        kafkaInstanceAPISecondaryUser.createAcl(acl);
    }

    @SneakyThrows
    @Test (dependsOnMethods = {"testAdminUserCanChangeTheKafkaInstanceOwner", "testPermissionsNewInstanceOwnerCanCreateACLs"})
    public void testNewInstanceOwnerCanDeleteACLs() {

        LOGGER.info("Test  new Instance owner (Secondary User) can create ACLs");
        // clean ACLs
        KafkaInstanceApiAccessUtils.removeAllButDefaultACLs(kafkaInstanceAPISecondaryUser, defaultPermissionsList);

        kafkaInstanceAPISecondaryUser.deleteAcls(
            AclResourceTypeFilter.TOPIC, "xx", null, null, null, null);
    }

    @Test (dependsOnMethods = "testAdminUserCanChangeTheKafkaInstanceOwner")
    @SneakyThrows
    public void testOldInstanceOwnerCanListACLs() {

        LOGGER.info("Test old instance owner (primary user) can list ACLs");
        kafkaInstanceAPIPrimaryUser.getAcls(null, null, null, null, null, null, null, null, null, null);
    }

    //// TODO unignore
    //@Ignore
    @Test(dependsOnMethods = "testAdminUserCanChangeTheKafkaInstanceOwner")
    public void testFormerOwnerCanNotDeleteTheKafkaInstance() {
        LOGGER.info("Test old instance owner (primary user) can list ACLs");
        // TODO possible problem at production.
        assertThrows(ApiNotFoundException.class, () -> primaryAPI.kafkaMgmt().deleteKafkaById(kafka.getId(), true));
    }

    @Test(dependsOnMethods = "testAdminUserCanChangeTheKafkaInstanceOwner")
    public void testAlienUserCanNotDeleteTheKafkaInstance() {
        assertThrows(ApiNotFoundException.class, () -> alienAPI.kafkaMgmt().deleteKafkaById(kafka.getId(), true));
    }

    @SneakyThrows
    // test is should be executed as last one.
    @Test(dependsOnMethods = "testAdminUserCanChangeTheKafkaInstanceOwner")
    public void testNewOwnerCanDeleteTheKafkaInstance() {
        KafkaMgmtApiUtils.cleanKafkaInstance(secondaryAPI.kafkaMgmt(), KAFKA_INSTANCE_NAME);
    }

}
