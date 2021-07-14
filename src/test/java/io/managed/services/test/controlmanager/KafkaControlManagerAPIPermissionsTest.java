package io.managed.services.test.controlmanager;

import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.exception.HTTPForbiddenException;
import io.managed.services.test.client.exception.HTTPNotFoundException;
import io.managed.services.test.client.exception.HTTPUnauthorizedException;
import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.kafkaadminapi.KafkaAdminAPIUtils;
import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.client.serviceapi.CreateServiceAccountPayload;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.message;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteKafkaByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteServiceAccountByNameIfExists;
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
 *  <li>unauth user:    SSO_UNAUTHORIZED_USERNAME, SSO_UNAUTHORIZED_PASSWORD</li>
 * </ul>
 * <p>
 * Conditions:
 * - The main user and secondary user should be part of the same organization
 * - The alien user should be part of a different organization as the main user
 * - The unauth user should not be allowed to use the API
 */
@Test(groups = TestTag.SERVICE_API_PERMISSIONS)
public class KafkaControlManagerAPIPermissionsTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(KafkaControlManagerAPIPermissionsTest.class);

    private static final String KAFKA_INSTANCE_NAME = "mk-e2e-up-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String SECONDARY_SERVICE_ACCOUNT_NAME = "mk-e2e-up-secondary-sa-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String ALIEN_SERVICE_ACCOUNT_NAME = "mk-e2e-up-alien-sa-" + Environment.KAFKA_POSTFIX_NAME;

    static final String FAKE_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUI" +
        "iwia2lkIiA6ICItNGVsY19WZE5fV3NPVVlmMkc0UXhyOEdjd0l4X0t0WFVDaXRhd" +
        "ExLbEx3In0.eyJleHAiOjE2MTM3MzI2NzAsImlhdCI6MTYxMzczMTc3MCwiYXV0a" +
        "F90aW1lIjoxNjEzNzMxNzY5LCJqdGkiOiIyZjAzYjI4Ni0yNWEzLTQyZjItOTdlY" +
        "S0zMjAwMjBjNWRkMzYiLCJpc3MiOiJodHRwczovL3Nzby5yZWRoYXQuY29tL2F1d" +
        "GgvcmVhbG1zL3JlZGhhdC1leHRlcm5hbCIsImF1ZCI6ImNsb3VkLXNlcnZpY2VzI" +
        "iwic3ViIjoiZjo1MjhkNzZmZi1mNzA4LTQzZWQtOGNkNS1mZTE2ZjRmZTBjZTY6b" +
        "WstdGVzdC11c2VyLWUyZS1wcmltYXJ5IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiY" +
        "2xvdWQtc2VydmljZXMiLCJzZXNzaW9uX3N0YXRlIjoiNWIzNzMzODktM2FhOC00Y" +
        "jExLTg2MTctOGYwNDQwM2Y2OTE5IiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2luc" +
        "yI6WyJodHRwczovL3Byb2QuZm9vLnJlZGhhdC5jb206MTMzNyIsImh0dHBzOi8vY" +
        "XBpLmNsb3VkLnJlZGhhdC5jb20iLCJodHRwczovL3FhcHJvZGF1dGguY2xvdWQuc" +
        "mVkaGF0LmNvbSIsImh0dHBzOi8vY2xvdWQub3BlbnNoaWZ0LmNvbSIsImh0dHBzO" +
        "i8vcHJvZC5mb28ucmVkaGF0LmNvbSIsImh0dHBzOi8vY2xvdWQucmVkaGF0LmNvb" +
        "SJdLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsiYXV0aGVudGljYXRlZCJdfSwic" +
        "2NvcGUiOiIiLCJhY2NvdW50X251bWJlciI6IjcwMjQ0MDciLCJpc19pbnRlcm5hb" +
        "CI6ZmFsc2UsImFjY291bnRfaWQiOiI1Mzk4ODU3NCIsImlzX2FjdGl2ZSI6dHJ1Z" +
        "Swib3JnX2lkIjoiMTQwMTQxNjEiLCJsYXN0X25hbWUiOiJVc2VyIiwidHlwZSI6I" +
        "lVzZXIiLCJsb2NhbGUiOiJlbl9VUyIsImZpcnN0X25hbWUiOiJUZXN0IiwiZW1ha" +
        "WwiOiJtay10ZXN0LXVzZXIrZTJlLXByaW1hcnlAcmVkaGF0LmNvbSIsInVzZXJuY" +
        "W1lIjoibWstdGVzdC11c2VyLWUyZS1wcmltYXJ5IiwiaXNfb3JnX2FkbWluIjpmY" +
        "WxzZX0.y0OHnHA8wLKPhpoeBp_8V4r76R6Miqdj6fNevWHOBsrJ4_j9GJJ2QfJme" +
        "TSY5V3d0nT2Rt2SZ9trPrLEFd3Wr5z9YGIle--TXKKkYKyyFr4FO8Uaxvh-oN45C" +
        "3cGsNYfbRBILqBCFHTmh54q1XoHA6FiteqdgMzUrBAoFG3SeFLl41u9abNA7EEe8" +
        "0ldozXsiSaLDWSylF1g9u1BhGqGuOpX0RoZGuTL_3KINEE7XoCbvW0xKecCA8-u1" +
        "C06X_GUgR0tVvdgoGpB9uPDX3sbqMpl7fNgJvwyZa8acVoJuxs5K945OYGzGXuDG" +
        "Gzt-zxEov9g4udCDxNQTUoHuCIrMrr1ubt2iFbqso4UF6h-NIbxqARxhlhhyH8U9" +
        "c2Zm1J_fLA9WJ8g1DJF75D66hV05s_RyRX1G6dFEriuT00PbGZQrxgH38zgZ8s-a" +
        "S3qCAc2vYS-ZD4_Sl2xQgICC1HYpbgUbWNeAVEOWygZJUPMJLgpJ3aM2P8Dnjia5" +
        "0KL0owSTYBWvFDkROI-ymDXfcRvEMVKyOdhljQNPZew4Ux4apBi9t-ncB9XabDo1" +
        "1eddbbmcV05FWDb8X4opshptnWDzAw4ZPhbjoTBhNEI2JbFssOSYpskNnkB4kKQb" +
        "BjVxAPldBNFwRKLOfvJNdY1jNurMY1xVMl2dbEpFBkqJf1lByU";

    private final Vertx vertx = Vertx.vertx();

    private ServiceAPI mainAPI;
    private ServiceAPI secondaryAPI;
    private ServiceAPI alienAPI;
    private KafkaResponse kafka;

    @BeforeClass(timeOut = 15 * MINUTES)
    public void bootstrap() throws Throwable {

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        mainAPI = bwait(ServiceAPIUtils.serviceAPI(vertx, Environment.SSO_USERNAME, Environment.SSO_PASSWORD));

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_SECONDARY_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        secondaryAPI = bwait(ServiceAPIUtils.serviceAPI(vertx, Environment.SSO_SECONDARY_USERNAME, Environment.SSO_SECONDARY_PASSWORD));

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_ALIEN_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        alienAPI = bwait(ServiceAPIUtils.serviceAPI(vertx, Environment.SSO_ALIEN_USERNAME, Environment.SSO_ALIEN_PASSWORD));

        LOGGER.info("create kafka instance: {}", KAFKA_INSTANCE_NAME);
        CreateKafkaPayload kafkaPayload = ServiceAPIUtils.createKafkaPayload(KAFKA_INSTANCE_NAME);
        kafka = bwait(ServiceAPIUtils.applyKafkaInstance(vertx, mainAPI, kafkaPayload));
    }

    @AfterClass(timeOut = DEFAULT_TIMEOUT, alwaysRun = true)
    public void teardown() throws Throwable {
        assumeTeardown();

        try {
            bwait(deleteKafkaByNameIfExists(mainAPI, KAFKA_INSTANCE_NAME));
        } catch (Throwable t) {
            LOGGER.error("clan kafka error: ", t);
        }

        try {
            bwait(deleteServiceAccountByNameIfExists(secondaryAPI, SECONDARY_SERVICE_ACCOUNT_NAME));
        } catch (Throwable t) {
            LOGGER.error("clean secondary service account error: ", t);
        }

        try {
            bwait(deleteServiceAccountByNameIfExists(alienAPI, ALIEN_SERVICE_ACCOUNT_NAME));
        } catch (Throwable t) {
            LOGGER.error("clean alien service account error: ", t);
        }

        bwait(vertx.close());
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testSecondaryUserCanReadTheKafkaInstance() throws Throwable {

        // Get kafka instance list by another user with same org
        LOGGER.info("fetch list of kafka instance from the secondary user in the same org");
        var kafkas = bwait(secondaryAPI.getListOfKafkas());

        var o = kafkas.items.stream()
            .filter(k -> k.name.equals(KAFKA_INSTANCE_NAME))
            .findAny();

        assertTrue(o.isPresent(), message("main user kafka is not visible for secondary user; kafkas: {}", Json.encode(kafkas.items)));
    }


    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testAlienUserCanNotReadTheKafkaInstance() throws Throwable {

        // Get list of kafka Instance in org 1 and test it should be there
        LOGGER.info("fetch list of kafka instance from the alin user in a different org");
        var kafkas = bwait(alienAPI.getListOfKafkas());

        var o = kafkas.items.stream()
            .filter(k -> k.name.equals(KAFKA_INSTANCE_NAME))
            .findAny();

        assertTrue(o.isEmpty(), message("main user kafka is visible for alien user; kafkas: {}", Json.encode(kafkas.items)));
    }

    /**
     * Use the secondary user to create a service account and consume and produce messages on the
     * kafka instance created by the main user
     */
    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testSecondaryUserCanNotCreateTopicOnTheKafkaInstance() throws Throwable {

        // Create Service Account by another user
        CreateServiceAccountPayload serviceAccountPayload = new CreateServiceAccountPayload();
        serviceAccountPayload.name = SECONDARY_SERVICE_ACCOUNT_NAME;

        LOGGER.info("create service account by another user with same org: {}", serviceAccountPayload.name);
        var serviceAccount = bwait(secondaryAPI.createServiceAccount(serviceAccountPayload));

        String bootstrapHost = kafka.bootstrapServerHost;
        String clientID = serviceAccount.clientID;
        String clientSecret = serviceAccount.clientSecret;

        // Create Kafka topic by another user
        LOGGER.info("initialize kafka admin; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        KafkaAdmin admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);

        String topicName = "test-topic";

        LOGGER.info("create kafka topic: {}", topicName);
        assertThrows(SaslAuthenticationException.class, () -> bwait(admin.createTopic(topicName)));

        admin.close();
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testAlienUserCanNotCreateTopicOnTheKafkaInstance() throws Throwable {

        LOGGER.info("initialize kafka admin api for kafka instance: {}", kafka.bootstrapServerHost);
        var admin = bwait(KafkaAdminAPIUtils.kafkaAdminAPI(vertx, kafka.bootstrapServerHost, Environment.SSO_ALIEN_USERNAME, Environment.SSO_ALIEN_PASSWORD));

        var topicName = "api-alien-test-topic";

        LOGGER.info("create kafka topic: {}", topicName);
        assertThrows(HTTPUnauthorizedException.class, () -> bwait(KafkaAdminAPIUtils.createDefaultTopic(admin, topicName)));
    }

    /**
     * A user in org A is not allowed to create topic to produce and consume messages on a kafka instance in org B
     */
    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testAlienUserCanNotCreateTopicOnTheKafkaInstanceUsingKafkaBin() throws Throwable {

        // Create Service Account of Org 2
        var serviceAccountPayload = new CreateServiceAccountPayload();
        serviceAccountPayload.name = ALIEN_SERVICE_ACCOUNT_NAME;

        LOGGER.info("create service account in alien org: {}", serviceAccountPayload.name);
        var serviceAccountOrg2 = bwait(alienAPI.createServiceAccount(serviceAccountPayload));


        String bootstrapHost = kafka.bootstrapServerHost;
        String clientID = serviceAccountOrg2.clientID;
        String clientSecret = serviceAccountOrg2.clientSecret;

        // Create Kafka topic in Org 1 from Org 2 and it should fail
        LOGGER.info("initialize kafka admin; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        KafkaAdmin admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);

        String topicName = "alien-test-topic";

        LOGGER.info("create kafka topic: {}", topicName);
        assertThrows(SaslAuthenticationException.class, () -> bwait(admin.createTopic(topicName)));

        admin.close();
    }

    @Test(priority = 1, timeOut = DEFAULT_TIMEOUT)
    public void testSecondaryUserCanNotDeleteTheKafkaInstance() {
        // should fail
        assertThrows(HTTPNotFoundException.class, () -> bwait(secondaryAPI.deleteKafka(kafka.id, true)));
    }

    @Test(priority = 1, timeOut = DEFAULT_TIMEOUT)
    public void testAlienUserCanNotDeleteTheKafkaInstance() {
        // should fail
        assertThrows(HTTPNotFoundException.class, () -> bwait(alienAPI.deleteKafka(kafka.id, true)));
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testUnauthorizedUser() throws Throwable {
        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_UNAUTHORIZED_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        var api = bwait(ServiceAPIUtils.serviceAPI(vertx, Environment.SSO_UNAUTHORIZED_USERNAME, Environment.SSO_UNAUTHORIZED_PASSWORD));
        assertThrows(HTTPForbiddenException.class, () -> bwait(api.getListOfKafkas()));
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testUnauthenticatedUserWithFakeToken() {
        var api = new ServiceAPI(vertx, Environment.SERVICE_API_URI, FAKE_TOKEN);
        assertThrows(HTTPUnauthorizedException.class, () -> bwait(api.getListOfKafkas()));
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testUnauthenticatedUserWithoutToken() {
        var api = new ServiceAPI(vertx, Environment.SERVICE_API_URI, "");
        assertThrows(HTTPUnauthorizedException.class, () -> bwait(api.getListOfKafkas()));
    }
}
