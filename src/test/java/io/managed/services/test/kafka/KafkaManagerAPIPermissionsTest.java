package io.managed.services.test.kafka;

import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.TopicSettings;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.ServiceAccountRequest;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.TestUtils;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.exception.ApiNotFoundException;
import io.managed.services.test.client.exception.ApiUnauthorizedException;
import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

/**
 * Test the User authn and authz for the control manager api.
 * <p>
 * This tests uses 4 users:
 * <ul>
 *  <li>main user:      SSO_USERNAME, SSO_PASSWORD</li>
 *  <li>secondary user: SSO_SECONDARY_USERNAME, SSO_SECONDARY_PASSWORD</li>
 *  <li>alien user:     SSO_ALIEN_USERNAME, SSO_ALIEN_PASSWORD</li>
 * </ul>
 * <p>
 * Conditions:
 * - The main user and secondary user should be part of the same organization
 * - The alien user should be part of a different organization as the main user
 */
@Test(groups = TestTag.SERVICE_API_PERMISSIONS)
public class KafkaManagerAPIPermissionsTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(KafkaManagerAPIPermissionsTest.class);

    private static final String KAFKA_INSTANCE_NAME = "mk-e2e-up-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String SECONDARY_SERVICE_ACCOUNT_NAME = "mk-e2e-up-secondary-sa-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String ALIEN_SERVICE_ACCOUNT_NAME = "mk-e2e-up-alien-sa-" + Environment.KAFKA_POSTFIX_NAME;

    private final Vertx vertx = Vertx.vertx();

    private ApplicationServicesApi mainAPI;
    private ApplicationServicesApi secondaryAPI;
    private ApplicationServicesApi alienAPI;

    private KafkaRequest kafka;

    @BeforeClass(timeOut = 15 * MINUTES)
    @SneakyThrows
    public void bootstrap() {

        mainAPI = ApplicationServicesApi.applicationServicesApi(
            Environment.SERVICE_API_URI,
            Environment.SSO_USERNAME,
            Environment.SSO_PASSWORD);

        secondaryAPI = ApplicationServicesApi.applicationServicesApi(
            Environment.SERVICE_API_URI,
            Environment.SSO_SECONDARY_USERNAME,
            Environment.SSO_SECONDARY_PASSWORD);

        alienAPI = ApplicationServicesApi.applicationServicesApi(
            Environment.SERVICE_API_URI,
            Environment.SSO_ALIEN_USERNAME,
            Environment.SSO_ALIEN_PASSWORD);

        LOGGER.info("create kafka instance '{}'", KAFKA_INSTANCE_NAME);
        kafka = KafkaMgmtApiUtils.applyKafkaInstance(mainAPI.kafkaMgmt(), KAFKA_INSTANCE_NAME);
    }

    @AfterClass(timeOut = DEFAULT_TIMEOUT, alwaysRun = true)
    @SneakyThrows
    public void teardown() {
        assumeTeardown();

        try {
            KafkaMgmtApiUtils.deleteKafkaByNameIfExists(mainAPI.kafkaMgmt(), KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            LOGGER.error("clan kafka error: ", t);
        }

        try {
            SecurityMgmtAPIUtils.deleteServiceAccountByNameIfExists(secondaryAPI.securityMgmt(), SECONDARY_SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean secondary service account error: ", t);
        }

        try {
            SecurityMgmtAPIUtils.deleteServiceAccountByNameIfExists(alienAPI.securityMgmt(), ALIEN_SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean alien service account error: ", t);
        }

        bwait(vertx.close());
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
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


    @Test(timeOut = DEFAULT_TIMEOUT)
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

    /**
     * Use the secondary user to create a service account and consume and produce messages on the
     * kafka instance created by the main user
     */
    @Test(timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testSecondaryUserCanNotCreateTopicOnTheKafkaInstance() {

        // TODO: Test using Kafka Bin and Kafka Admin

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
        var admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);

        var topicName = "test-topic";

        LOGGER.info("create kafka topic '{}'", topicName);
        assertThrows(SaslAuthenticationException.class, () -> admin.createTopic(topicName));

        admin.close();
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testAlienUserCanNotCreateTopicOnTheKafkaInstance() {

        LOGGER.info("initialize kafka admin api for kafka instance: {}", kafka.getBootstrapServerHost());
        var kafkaInstanceApi = bwait(KafkaInstanceApiUtils.kafkaInstanceApi(kafka,
            Environment.SSO_ALIEN_USERNAME,
            Environment.SSO_ALIEN_PASSWORD));

        var topicPayload = new NewTopicInput()
            .name("api-alien-test-topic")
            .settings(new TopicSettings().numPartitions(1));
        LOGGER.info("create kafka topic: {}", topicPayload.getName());
        assertThrows(ApiUnauthorizedException.class, () -> kafkaInstanceApi.createTopic(topicPayload));
    }

    /**
     * A user in org A is not allowed to create topic to produce and consume messages on a kafka instance in org B
     */
    @Test(timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testAlienUserCanNotCreateTopicOnTheKafkaInstanceUsingKafkaBin() {

        // Create Service Account of Org 2
        var serviceAccountPayload = new ServiceAccountRequest()
            .name(SECONDARY_SERVICE_ACCOUNT_NAME);

        LOGGER.info("create service account in alien org: {}", serviceAccountPayload.getName());
        var serviceAccountOrg2 = alienAPI.securityMgmt().createServiceAccount(serviceAccountPayload);


        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccountOrg2.getClientId();
        var clientSecret = serviceAccountOrg2.getClientSecret();

        // Create Kafka topic in Org 1 from Org 2, and it should fail
        LOGGER.info("initialize kafka admin; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        var admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);

        var topicName = "alien-test-topic";

        LOGGER.info("create kafka topic: {}", topicName);
        assertThrows(SaslAuthenticationException.class, () -> admin.createTopic(topicName));

        admin.close();
    }

    @Test(priority = 1, timeOut = DEFAULT_TIMEOUT)
    public void testSecondaryUserCanNotDeleteTheKafkaInstance() {
        // should fail
        assertThrows(ApiNotFoundException.class, () -> secondaryAPI.kafkaMgmt().deleteKafkaById(kafka.getId(), true));
    }

    @Test(priority = 1, timeOut = DEFAULT_TIMEOUT)
    public void testAlienUserCanNotDeleteTheKafkaInstance() {
        // should fail
        assertThrows(ApiNotFoundException.class, () -> alienAPI.kafkaMgmt().deleteKafkaById(kafka.getId(), true));
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testUnauthenticatedUserWithFakeToken() {
        var api = new ApplicationServicesApi(Environment.SERVICE_API_URI, TestUtils.FAKE_TOKEN);
        assertThrows(ApiUnauthorizedException.class, () -> api.kafkaMgmt().getKafkas(null, null, null, null));
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testUnauthenticatedUserWithoutToken() {
        var api = new ApplicationServicesApi(Environment.SERVICE_API_URI, "");
        assertThrows(ApiUnauthorizedException.class, () -> api.kafkaMgmt().getKafkas(null, null, null, null));
    }
}
