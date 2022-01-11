package io.managed.services.test.kafka;

import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.TopicSettings;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.MetricsInstantQueryList;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import com.openshift.cloud.api.kas.models.ServiceAccountRequest;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.TestUtils;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.exception.ApiForbiddenException;
import io.managed.services.test.client.exception.ApiNotFoundException;
import io.managed.services.test.client.exception.ApiUnauthorizedException;
import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.oauth.KeycloakUser;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import lombok.SneakyThrows;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

/**
 * Test the User authn and authz for the Kafka Mgmt API.
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
public class KafkaMgmtAPIPermissionsTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(KafkaMgmtAPIPermissionsTest.class);

    private static final String KAFKA_INSTANCE_NAME = "mk-e2e-up-" + Environment.LAUNCH_KEY;
    private static final String PRIMARY_SERVICE_ACCOUNT_NAME = "mk-e2e-up-primary-sa-" + Environment.LAUNCH_KEY;
    private static final String SECONDARY_SERVICE_ACCOUNT_NAME = "mk-e2e-up-secondary-sa-" + Environment.LAUNCH_KEY;
    private static final String ALIEN_SERVICE_ACCOUNT_NAME = "mk-e2e-up-alien-sa-" + Environment.LAUNCH_KEY;



    private ApplicationServicesApi mainAPI;
    private ApplicationServicesApi secondaryAPI;
    private ApplicationServicesApi alienAPI;
    private ApplicationServicesApi adminAPI;

    private KafkaRequest kafka;

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

        mainAPI = ApplicationServicesApi.applicationServicesApi(
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
        kafka = KafkaMgmtApiUtils.applyKafkaInstance(mainAPI.kafkaMgmt(), KAFKA_INSTANCE_NAME);
    }

    @AfterClass(alwaysRun = true)
    @SneakyThrows
    public void teardown() {
        assumeTeardown();

        try {
            KafkaMgmtApiUtils.cleanKafkaInstance(adminAPI.kafkaMgmt(), KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            LOGGER.error("clan kafka error: ", t);
        }
        try {
            SecurityMgmtAPIUtils.cleanServiceAccount(mainAPI.securityMgmt(), PRIMARY_SERVICE_ACCOUNT_NAME);
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

    @Test
    @SneakyThrows
    public void testSecondaryUserCanNotCreateTopicOnTheKafkaInstance() {

        LOGGER.info("initialize kafka admin api for kafka instance: {}", kafka.getBootstrapServerHost());
        var kafkaInstanceApi = bwait(KafkaInstanceApiUtils.kafkaInstanceApi(kafka,
            Environment.SECONDARY_USERNAME,
            Environment.SECONDARY_PASSWORD));

        var topicPayload = new NewTopicInput()
            .name("api-secondary-test-topic")
            .settings(new TopicSettings().numPartitions(1));
        LOGGER.info("create kafka topic: {}", topicPayload.getName());
        assertThrows(ApiForbiddenException.class, () -> kafkaInstanceApi.createTopic(topicPayload));
    }


    /**
     * Use the secondary user to create a service account and consume and produce messages on the
     * kafka instance created by the main user
     */
    @Test
    @SneakyThrows
    public void testSecondaryUserServiceAccountCanNotCreateTopicOnTheKafkaInstance() {

        // Create Service Account by another user
        var serviceAccountPayload = new ServiceAccountRequest()
            .name(SECONDARY_SERVICE_ACCOUNT_NAME);

        LOGGER.info("create service account by another user with same org: {}", serviceAccountPayload.getName());
        var serviceAccount = secondaryAPI.securityMgmt().createServiceAccount(serviceAccountPayload);

        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccount.getClientId();
        var clientSecret = serviceAccount.getClientSecret();

        // Create Kafka topic by another user
        LOGGER.info("initialize kafka admin; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        try (var admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret)) {

            var topicName = "secondary-test-topic";

            LOGGER.info("create kafka topic '{}'", topicName);
            assertThrows(TopicAuthorizationException.class, () -> admin.createTopic(topicName));
        }
    }

    @Test
    @SneakyThrows
    public void testAlienUserCanNotCreateTopicOnTheKafkaInstance() {

        LOGGER.info("initialize kafka admin api for kafka instance: {}", kafka.getBootstrapServerHost());
        var kafkaInstanceApi = bwait(KafkaInstanceApiUtils.kafkaInstanceApi(kafka,
            Environment.ALIEN_USERNAME,
            Environment.ALIEN_PASSWORD));

        var topicPayload = new NewTopicInput()
            .name("api-alien-test-topic")
            .settings(new TopicSettings().numPartitions(1));
        LOGGER.info("create kafka topic: {}", topicPayload.getName());
        assertThrows(ApiUnauthorizedException.class, () -> kafkaInstanceApi.createTopic(topicPayload));
    }

    /**
     * A user in org A is not allowed to create topic to produce and consume messages on a kafka instance in org B
     */
    @Test
    @SneakyThrows
    public void testAlienUserServiceAccountCanNotCreateTopicOnTheKafkaInstance() {

        // Create Service Account of Org 2
        var serviceAccountPayload = new ServiceAccountRequest()
            .name(ALIEN_SERVICE_ACCOUNT_NAME);

        LOGGER.info("create service account in alien org: {}", serviceAccountPayload.getName());
        var serviceAccountOrg2 = alienAPI.securityMgmt().createServiceAccount(serviceAccountPayload);


        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccountOrg2.getClientId();
        var clientSecret = serviceAccountOrg2.getClientSecret();

        // Create Kafka topic in Org 1 from Org 2, and it should fail
        LOGGER.info("initialize kafka admin; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        try (var admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret)) {

            var topicName = "alien-test-topic";

            LOGGER.info("create kafka topic: {}", topicName);
            assertThrows(SaslAuthenticationException.class, () -> admin.createTopic(topicName));
        }
    }

    @Test(priority = 1)
    public void testSecondaryUserCanNotDeleteTheKafkaInstance() {
        // should failKafkaControlManagerAPIPermissionsTestKafkaControlManagerAPIPermissionsTest
        assertThrows(ApiNotFoundException.class, () -> secondaryAPI.kafkaMgmt().deleteKafkaById(kafka.getId(), true));
    }

    @Test(priority = 1)
    public void testAlienUserCanNotDeleteTheKafkaInstance() {
        // should fail
        assertThrows(ApiNotFoundException.class, () -> alienAPI.kafkaMgmt().deleteKafkaById(kafka.getId(), true));
    }

    @Test
    public void testUnauthenticatedUserWithFakeToken() {
        var api = new ApplicationServicesApi(Environment.OPENSHIFT_API_URI, new KeycloakUser(TestUtils.FAKE_TOKEN));
        assertThrows(ApiUnauthorizedException.class, () -> api.kafkaMgmt().getKafkas(null, null, null, null));
    }

    @Test
    public void testUnauthenticatedUserWithoutToken() {
        var api = new ApplicationServicesApi(Environment.OPENSHIFT_API_URI, new KeycloakUser(""));
        assertThrows(ApiUnauthorizedException.class, () -> api.kafkaMgmt().getKafkas(null, null, null, null));
    }

    @SneakyThrows
    @Test
    public void testAdminUserCanResetTheServiceAccountCredentials() {
        // Getting secret of Some service account within organization
        ServiceAccount serviceAccountOriginal = SecurityMgmtAPIUtils.applyServiceAccount(mainAPI.securityMgmt(), PRIMARY_SERVICE_ACCOUNT_NAME);
        String secretOriginal  = serviceAccountOriginal.getClientSecret();

        // Resetting of secret
        ServiceAccount serviceAccountNew = adminAPI.securityMgmt().resetServiceAccountCreds(serviceAccountOriginal.getId());
        String secretNew = serviceAccountNew.getClientSecret();

        assertNotEquals(secretNew, secretOriginal);
    }

    @SneakyThrows
    @Test
    public void testAdminUserCanDeleteTheServiceAccount() {
        // Getting or creating
        ServiceAccount serviceAccountOriginal = SecurityMgmtAPIUtils.applyServiceAccount(secondaryAPI.securityMgmt(), SECONDARY_SERVICE_ACCOUNT_NAME);

        // Deletion of Service account
        adminAPI.securityMgmt().deleteServiceAccountById(serviceAccountOriginal.getId());

        // ServiceAccount should no exist by this time
        assertThrows(
                ApiNotFoundException.class,
                () -> adminAPI.securityMgmt().getServiceAccountById(serviceAccountOriginal.getId())
        );
    }

    @SneakyThrows
    @Test
    public void testAdminUserCanReadUserMetricsOfTheKafkaInstance() {
        MetricsInstantQueryList response = adminAPI.kafkaMgmt().getMetricsByInstantQuery(kafka.getId(), null);
        assertNotNull(response);
    }

    @SneakyThrows
    @Test (priority = 2)
    public void testAdminUserCanChangeTheKafkaInstanceOwner() {

        var authorizationTopicName = "authorization-topic-name";
        LOGGER.info("initialize kafka admin api for kafka instance: {}", kafka.getBootstrapServerHost());
        var kafkaInstanceAPISecondaryUser = bwait(KafkaInstanceApiUtils.kafkaInstanceApi(kafka,
                Environment.SECONDARY_USERNAME,
                Environment.SECONDARY_PASSWORD));

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
    @Test (priority = 3)
    // test is should be executed as last one.
    public void testAdminUserCanDeleteTheKafkaInstance() {
        KafkaMgmtApiUtils.cleanKafkaInstance(adminAPI.kafkaMgmt(), KAFKA_INSTANCE_NAME);
    }

}

