package io.managed.services.test.devexp;


import com.openshift.cloud.v1alpha.models.CloudServiceAccountRequest;
import com.openshift.cloud.v1alpha.models.CloudServiceAccountRequestSpec;
import com.openshift.cloud.v1alpha.models.CloudServicesRequest;
import com.openshift.cloud.v1alpha.models.CloudServicesRequestSpec;
import com.openshift.cloud.v1alpha.models.Credentials;
import com.openshift.cloud.v1alpha.models.KafkaConnection;
import com.openshift.cloud.v1alpha.models.KafkaConnectionSpec;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.managed.services.test.Environment;
import io.managed.services.test.IsReady;
import io.managed.services.test.TestBase;
import io.managed.services.test.TestUtils;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.framework.LogCollector;
import io.managed.services.test.framework.TestTag;
import io.managed.services.test.kafkainstances.LongLiveKafkaInstanceTest;
import io.managed.services.test.operator.OperatorUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.ext.auth.User;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;
import org.testng.ITestContext;
import org.testng.TestException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.message;
import static io.managed.services.test.TestUtils.waitFor;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteServiceAccountByNameIfExists;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.testng.Assert.assertNotNull;


/**
 * Test the application services operator[1] kafka operations.
 *
 * The tests expect the operator to be already installed on the dev cluster, the dev cluster is given by
 * the DEV_CLUSTER_SERVER env. The tested CRs will be created in the DEV_CLUSTER_NAMESPACE set namespace.
 *
 * The DEV_CLUSTER_NAMESPACE can and the DEV_CLUSTER_TOKEN should be created using
 * the ./hack/bootstrap-mk-e2e-tests-namespace.sh script.
 *
 * The operator must be configured to target the desired environment by manually setting the CLOUD_SERVICES_API
 * on the operator installed ClusterServiceVersion.
 *
 * 1. https://github.com/redhat-developer/app-services-operator
 */
@Test(groups = TestTag.BINDING_OPERATOR)
public class KafkaOperatorTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(KafkaOperatorTest.class);

    // use the kafka long living instance
    // TODO: Make KafkaOperatorTest independent from LongLiveKafkaTest
    private static final String KAFKA_INSTANCE_NAME = LongLiveKafkaInstanceTest.KAFKA_INSTANCE_NAME;

    private final Vertx vertx = Vertx.vertx();

    private User user;
    private ServiceAPI api;
    private KubernetesClient client;

    private CloudServicesRequest cloudServicesRequest;

    private final static String ACCESS_TOKEN_SECRET_NAME = "mk-e2e-api-accesstoken";
    private final static String CLOUD_SERVICE_ACCOUNT_REQUEST_NAME = "mk-e2e-service-account-request";
    private final static String SERVICE_ACCOUNT_NAME = "mk-e2e-bo-sa-" + Environment.KAFKA_POSTFIX_NAME;
    private final static String SERVICE_ACCOUNT_SECRET_NAME = "mk-e2e-service-account-secret";
    private final static String CLOUD_SERVICES_REQUEST_NAME = "mk-e2e-kafka-request";
    private final static String KAFKA_CONNECTION_NAME = "mk-e2e-kafka-connection";

    private void assertENVs() {
        assertNotNull(Environment.SSO_USERNAME, "the SSO_USERNAME env is null");
        assertNotNull(Environment.SSO_PASSWORD, "the SSO_PASSWORD env is null");
        assertNotNull(Environment.DEV_CLUSTER_SERVER, "the DEV_CLUSTER_SERVER env is null");
        assertNotNull(Environment.DEV_CLUSTER_TOKEN, "the DEV_CLUSTER_TOKEN env is null");
    }

    private Future<Void> bootstrapUser(Vertx vertx) {

        var auth = new KeycloakOAuth(vertx);

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        return auth.login(
            Environment.SSO_REDHAT_KEYCLOAK_URI,
            Environment.SSO_REDHAT_REDIRECT_URI,
            Environment.SSO_REDHAT_REALM,
            Environment.SSO_REDHAT_CLIENT_ID,
            Environment.SSO_USERNAME,
            Environment.SSO_PASSWORD)

            .onSuccess(u -> user = u)
            .map(__ -> null);
    }

    private void bootstrapAPI(Vertx vertx) {
        api = new ServiceAPI(vertx, Environment.SERVICE_API_URI, user);
    }

    private void bootstrapK8sClient() {

        Config config = new ConfigBuilder()
            .withMasterUrl(Environment.DEV_CLUSTER_SERVER)
            .withOauthToken(Environment.DEV_CLUSTER_TOKEN)
            .withNamespace(Environment.DEV_CLUSTER_NAMESPACE)
            .build();

        LOGGER.info("initialize kubernetes client");
        client = new DefaultKubernetesClient(config);
    }

    @BeforeClass(timeOut = DEFAULT_TIMEOUT)
    public void bootstrap() throws Throwable {
        assertENVs();

        bwait(bootstrapUser(vertx));

        bootstrapAPI(vertx);

        bootstrapK8sClient();
    }

    private void cleanAccessTokenSecret() {
        Secret s = client.secrets().withName(ACCESS_TOKEN_SECRET_NAME).get();
        if (s != null) {
            LOGGER.info("clean secret: {}", s.getMetadata().getName());
            client.secrets().delete(s);
        }
    }

    private void cleanCloudServiceAccountRequest() {
        var a = OperatorUtils.cloudServiceAccountRequest(client).withName(CLOUD_SERVICE_ACCOUNT_REQUEST_NAME).get();
        if (a != null) {
            LOGGER.info("clean CloudServiceAccountRequest: {}", a.getMetadata().getName());
            OperatorUtils.cloudServiceAccountRequest(client).delete(a);
        }
    }

    private void cleanCloudServicesRequest() {
        var k = OperatorUtils.cloudServicesRequest(client).withName(CLOUD_SERVICES_REQUEST_NAME).get();
        if (k != null) {
            LOGGER.info("clean CloudServicesRequest: {}", k.getMetadata().getName());
            OperatorUtils.cloudServicesRequest(client).delete(k);
        }
    }

    private void cleanKafkaConnection() {
        var c = OperatorUtils.kafkaConnection(client).withName(KAFKA_CONNECTION_NAME).get();
        if (c != null) {
            LOGGER.info("clean ManagedKafkaConnection: {}", c.getMetadata().getName());
            OperatorUtils.kafkaConnection(client).delete(c);
        }
    }

    private void collectOperatorLogs(ITestContext context) throws IOException {
        LogCollector.saveDeploymentLog(
            TestUtils.getLogPath(Environment.LOG_DIR.resolve("test-logs").toString(), context),
            client,
            "openshift-operators",
            "service-binding-operator");

    }

    private Future<Void> cleanServiceAccount() {
        return deleteServiceAccountByNameIfExists(api, SERVICE_ACCOUNT_NAME);
    }

    @AfterClass(timeOut = DEFAULT_TIMEOUT, alwaysRun = true)
    public void teardown(ITestContext context) throws Throwable {
        assumeTeardown();

        try {
            cleanKafkaConnection();
        } catch (Exception e) {
            LOGGER.error("clean kafka connection error: ", e);
        }

        try {
            cleanCloudServicesRequest();
        } catch (Exception e) {
            LOGGER.error("clean cloud services request error: ", e);
        }

        try {
            cleanCloudServiceAccountRequest();
        } catch (Exception e) {
            LOGGER.error("clean cloud service account request error: ", e);
        }

        try {
            cleanAccessTokenSecret();
        } catch (Exception e) {
            LOGGER.error("clean access token secret error: ", e);
        }

        try {
            collectOperatorLogs(context);
        } catch (Exception e) {
            LOGGER.error("collect operator logs error: ", e);
        }

        // force clean the service account if it hasn't done it yet
        try {
            bwait(cleanServiceAccount());
        } catch (Throwable t) {
            LOGGER.error("cleanServiceAccount error: ", t);
        }

        bwait(vertx.close());
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testCreateAccessTokenSecret() {

        // Create Secret
        Map<String, String> data = new HashMap<>();
        data.put("value", Base64.getEncoder().encodeToString(KeycloakOAuth.getRefreshToken(user).getBytes()));

        LOGGER.info("create access token secret with name: {}", ACCESS_TOKEN_SECRET_NAME);
        client.secrets().create(OperatorUtils.buildSecret(ACCESS_TOKEN_SECRET_NAME, data));
    }

    @Test(dependsOnMethods = "testCreateAccessTokenSecret", timeOut = DEFAULT_TIMEOUT)
    public void testCreateCloudServiceAccountRequest() throws Throwable {

        var a = new CloudServiceAccountRequest();
        a.getMetadata().setName(CLOUD_SERVICE_ACCOUNT_REQUEST_NAME);
        a.setSpec(new CloudServiceAccountRequestSpec());
        a.getSpec().setServiceAccountName(SERVICE_ACCOUNT_NAME);
        a.getSpec().setServiceAccountDescription("");
        a.getSpec().setServiceAccountSecretName(SERVICE_ACCOUNT_SECRET_NAME);
        a.getSpec().setAccessTokenSecretName(ACCESS_TOKEN_SECRET_NAME);

        LOGGER.info("create CloudServiceAccountRequest with name: {}", CLOUD_SERVICE_ACCOUNT_REQUEST_NAME);
        a = OperatorUtils.cloudServiceAccountRequest(client).create(a);
        LOGGER.info("created CloudServiceAccountRequest: {}", Json.encode(a));

        IsReady<CloudServiceAccountRequest> ready = last -> Future.succeededFuture(OperatorUtils.cloudServiceAccountRequest(client).withName(CLOUD_SERVICE_ACCOUNT_REQUEST_NAME).get())
            .map(r -> {

                LOGGER.info("CloudServiceAccountRequest status is: {}", Json.encode(r.getStatus()));

                if (last) {
                    LOGGER.warn("last CloudServiceAccountRequest is: {}", Json.encode(r));
                }

                if (r.getStatus() != null && r.getStatus().getMessage().equals("Created")) {
                    return Pair.with(true, r);
                }
                return Pair.with(false, null);
            });

        var cloudServiceAccountRequest = bwait(waitFor(vertx, "CloudServiceAccountRequest to complete", ofSeconds(10), ofMinutes(4), ready));
        LOGGER.info("CloudServiceAccountRequest is ready: {}", Json.encode(cloudServiceAccountRequest));
    }

    @Test(dependsOnMethods = "testCreateAccessTokenSecret", timeOut = DEFAULT_TIMEOUT)
    public void testCreateCloudServicesRequest() throws Throwable {

        var k = new CloudServicesRequest();
        k.getMetadata().setName(CLOUD_SERVICES_REQUEST_NAME);
        k.setSpec(new CloudServicesRequestSpec());
        k.getSpec().setAccessTokenSecretName(ACCESS_TOKEN_SECRET_NAME);

        LOGGER.info("create CloudServicesRequest with name: {}", CLOUD_SERVICES_REQUEST_NAME);
        k = OperatorUtils.cloudServicesRequest(client).create(k);
        LOGGER.info("created CloudServicesRequest: {}", Json.encode(k));

        IsReady<CloudServicesRequest> ready = last -> Future.succeededFuture(OperatorUtils.cloudServicesRequest(client).withName(CLOUD_SERVICES_REQUEST_NAME).get())
            .map(r -> {

                LOGGER.info("CloudServicesRequest status is: {}", Json.encode(r.getStatus()));

                if (last) {
                    LOGGER.warn("last CloudServicesRequest is: {}", Json.encode(r));
                }

                if (r.getStatus() != null
                    && r.getStatus().getUserKafkas() != null
                    && !r.getStatus().getUserKafkas().isEmpty()) {

                    return Pair.with(true, r);
                }
                return Pair.with(false, null);
            });
        cloudServicesRequest = bwait(waitFor(vertx, "CloudServicesRequest to complete", ofSeconds(10), ofMinutes(3), ready));
        LOGGER.info("CloudServicesRequest is ready: {}", Json.encode(cloudServicesRequest));
    }

    @Test(dependsOnMethods = {"testCreateCloudServiceAccountRequest", "testCreateCloudServicesRequest"}, timeOut = DEFAULT_TIMEOUT)
    public void testCreateManagedKafkaConnection() throws Throwable {

        var userKafka = cloudServicesRequest.getStatus().getUserKafkas().stream()
            .filter(k -> k.getName().equals(KAFKA_INSTANCE_NAME))
            .findFirst();

        if (userKafka.isEmpty()) {
            LOGGER.info("CloudServicesRequest: {}", Json.encode(cloudServicesRequest));
            throw new TestException(message("failed to find the user kafka instance {} in the CloudServicesRequest {}", KAFKA_INSTANCE_NAME, CLOUD_SERVICES_REQUEST_NAME));
        }

        var c = new KafkaConnection();
        c.getMetadata().setName(KAFKA_CONNECTION_NAME);
        c.setSpec(new KafkaConnectionSpec());
        c.getSpec().setAccessTokenSecretName(ACCESS_TOKEN_SECRET_NAME);
        c.getSpec().setKafkaId(userKafka.orElseThrow().getId());
        c.getSpec().setCredentials(new Credentials(SERVICE_ACCOUNT_SECRET_NAME));

        LOGGER.info("create ManagedKafkaConnection with name: {}", KAFKA_CONNECTION_NAME);
        c = OperatorUtils.kafkaConnection(client).create(c);
        LOGGER.info("created ManagedKafkaConnection: {}", Json.encode(c));

        IsReady<KafkaConnection> ready = last -> Future.succeededFuture(OperatorUtils.kafkaConnection(client).withName(KAFKA_CONNECTION_NAME).get())
            .map(r -> {

                LOGGER.info("ManagedKafkaConnection status is: {}", Json.encode(r.getStatus()));

                if (last) {
                    LOGGER.warn("last ManagedKafkaConnection is: {}", Json.encode(r));
                }

                if (r.getStatus() != null
                    && r.getStatus().getMessage() != null
                    && r.getStatus().getMessage().equals("Created")) {

                    return Pair.with(true, r);
                }
                return Pair.with(false, null);
            });
        var r = bwait(waitFor(vertx, "ManagedKafkaConnection to complete", ofSeconds(10), ofMinutes(2), ready));
        LOGGER.info("ManagedKafkaConnection is ready: {}", Json.encode(r));
    }
}
