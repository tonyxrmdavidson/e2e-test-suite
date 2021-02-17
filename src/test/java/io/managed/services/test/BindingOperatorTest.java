package io.managed.services.test;


import com.openshift.cloud.v1alpha.models.Credentials;
import com.openshift.cloud.v1alpha.models.ManagedKafkaConnection;
import com.openshift.cloud.v1alpha.models.ManagedKafkaConnectionSpec;
import com.openshift.cloud.v1alpha.models.ManagedKafkaRequest;
import com.openshift.cloud.v1alpha.models.ManagedKafkaRequestSpec;
import com.openshift.cloud.v1alpha.models.ManagedServiceAccountRequest;
import com.openshift.cloud.v1alpha.models.ManagedServiceAccountRequestSpec;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.framework.TestTag;
import io.managed.services.test.operator.OperatorUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.ext.auth.User;
import io.vertx.junit5.VertxExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static io.managed.services.test.TestUtils.await;
import static io.managed.services.test.TestUtils.waitFor;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(TestTag.CI)
@Tag(TestTag.BINDING_OPERATOR)
@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Disabled("We need to find a long term cluster for this tests")
public class BindingOperatorTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(BindingOperatorTest.class);

    User user;

    ServiceAPI api;
    KubernetesClient client;

    final static String ACCESS_TOKEN_SECRET_NAME = "mk-e2e-api-accesstoken";
    final static String MANAGED_SERVICE_ACCOUNT_REQUEST_NAME = "mk-e2e-service-account-request";
    final static String SERVICE_ACCOUNT_NAME = "mk-e2e-service-account";
    final static String SERVICE_ACCOUNT_SECRET_NAME = "mk-e2e-service-account-secret";
    final static String MANAGED_KAFKA_REQUEST_NAME = "mk-e2e-kafka-request";
    final static String MANAGED_KAFKA_CONNECTION_NAME = "mk-e2e-kafka-connection";

    @BeforeAll
    void bootstrap(Vertx vertx) {
        assumeTrue(Environment.SSO_USERNAME != null, "the SSO_USERNAME env is null");
        assumeTrue(Environment.SSO_PASSWORD != null, "the SSO_PASSWORD env is null");
        assumeTrue(Environment.DEV_CLUSTER_TOKEN != null, "the DEV_CLUSTER_TOKEN env is null");

        KeycloakOAuth auth = new KeycloakOAuth(vertx,
            Environment.SSO_REDHAT_KEYCLOAK_URI,
            Environment.SSO_REDHAT_REDIRECT_URI,
            Environment.SSO_REDHAT_REALM,
            Environment.SSO_REDHAT_CLIENT_ID);

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        this.user = await(auth.login(Environment.SSO_USERNAME, Environment.SSO_PASSWORD));
        this.api = new ServiceAPI(vertx, Environment.SERVICE_API_URI, user);


        Config config = new ConfigBuilder()
            .withMasterUrl(Environment.DEV_CLUSTER_SERVER)
            .withOauthToken(Environment.DEV_CLUSTER_TOKEN)
            .withNamespace(Environment.DEV_CLUSTER_NAMESPACE)
            .build();

        LOGGER.info("initialize kubernetes client");
        this.client = new DefaultKubernetesClient(config);
    }

    @AfterAll
    void cleanAccessTokenSecret() {
        Secret s = client.secrets().withName(ACCESS_TOKEN_SECRET_NAME).get();
        if (s != null) {
            LOGGER.info("clean secret: {}", s.getMetadata().getName());
            client.secrets().delete(s);
        }
    }

    @AfterAll
    void cleanManagedServiceAccountRequest() {
        ManagedServiceAccountRequest a = OperatorUtils.managedServiceAccount(client).withName(MANAGED_SERVICE_ACCOUNT_REQUEST_NAME).get();
        if (a != null) {
            LOGGER.info("clean ManagedServiceAccountRequest: {}", a.getMetadata().getName());
            OperatorUtils.managedServiceAccount(client).delete(a);
        }
    }

    @AfterAll
    void cleanManagedKafkaRequest() {
        var k = OperatorUtils.managedKafka(client).withName(MANAGED_KAFKA_REQUEST_NAME).get();
        if (k != null) {
            LOGGER.info("clean ManagedKafkaRequest: {}", k.getMetadata().getName());
            OperatorUtils.managedKafka(client).delete(k);
        }
    }

    @AfterAll
    void cleanManagedKafkaConnection() {
        var c = OperatorUtils.managedKafkaConnection(client).withName(MANAGED_KAFKA_CONNECTION_NAME).get();
        if (c != null) {
            LOGGER.info("clean ManagedKafkaConnection: {}", c.getMetadata().getName());
            OperatorUtils.managedKafkaConnection(client).delete(c);
        }
    }

    // TODO: Collect the operator logs AfterAll

    @Test
    @Order(1)
    void createAccessTokenSecret() {
        assumeTrue(client != null, "the global client is null");

        // Create Secret
        Map<String, String> data = new HashMap<>();
        data.put("value", Base64.getEncoder().encodeToString(KeycloakOAuth.getRefreshToken(user).getBytes()));

        LOGGER.info("create access token secret with name: {}", ACCESS_TOKEN_SECRET_NAME);
        client.secrets().create(OperatorUtils.buildSecret(ACCESS_TOKEN_SECRET_NAME, data));
    }

    @Test
    @Order(2)
    void createManagedServiceAccountRequest(Vertx vertx) {
        assumeTrue(client != null, "the global client is null");

        var a = new ManagedServiceAccountRequest();
        a.getMetadata().setName(MANAGED_SERVICE_ACCOUNT_REQUEST_NAME);
        a.setSpec(new ManagedServiceAccountRequestSpec());
        a.getSpec().setServiceAccountName(SERVICE_ACCOUNT_NAME);
        a.getSpec().setServiceAccountDescription("");
        a.getSpec().setServiceAccountSecretName(SERVICE_ACCOUNT_SECRET_NAME);
        a.getSpec().setAccessTokenSecretName(ACCESS_TOKEN_SECRET_NAME);

        LOGGER.info("create ManagedServiceAccountRequest with name: {}", MANAGED_SERVICE_ACCOUNT_REQUEST_NAME);
        a = OperatorUtils.managedServiceAccount(client).create(a);
        LOGGER.info("created ManagedServiceAccountRequest: {}", Json.encode(a));

        IsReady<ManagedServiceAccountRequest> ready = last ->
            Future.succeededFuture(OperatorUtils.managedServiceAccount(client).withName(MANAGED_SERVICE_ACCOUNT_REQUEST_NAME).get())
                .map(r -> {

                    LOGGER.info("ManagedServiceAccountRequest status is: {}", Json.encode(r.getStatus()));

                    if (last) {
                        LOGGER.warn("last ManagedServiceAccountRequest is: {}", Json.encode(r));
                    }

                    if (r.getStatus() != null && r.getStatus().getMessage().equals("Created")) {
                        return Pair.with(true, r);
                    }
                    return Pair.with(false, null);
                });
        a = await(waitFor(vertx, "ManagedServiceAccountRequest to complete", ofSeconds(10), ofMinutes(2), ready));
        LOGGER.info("ManagedServiceAccountRequest is ready: {}", Json.encode(a));
    }

    @Test
    @Order(2)
    void createManagedKafkaRequest(Vertx vertx) {
        assumeTrue(client != null, "the global client is null");

        var k = new ManagedKafkaRequest();
        k.getMetadata().setName(MANAGED_KAFKA_REQUEST_NAME);
        k.setSpec(new ManagedKafkaRequestSpec());
        k.getSpec().setAccessTokenSecretName(ACCESS_TOKEN_SECRET_NAME);

        LOGGER.info("create ManagedKafkaRequest with name: {}", MANAGED_KAFKA_REQUEST_NAME);
        k = OperatorUtils.managedKafka(client).create(k);
        LOGGER.info("created ManagedKafkaRequest: {}", Json.encode(k));

        IsReady<ManagedKafkaRequest> ready = last ->
            Future.succeededFuture(OperatorUtils.managedKafka(client).withName(MANAGED_KAFKA_REQUEST_NAME).get())
                .map(r -> {

                    LOGGER.info("ManagedKafkaRequest status is: {}", Json.encode(r.getStatus()));

                    if (last) {
                        LOGGER.warn("last ManagedKafkaRequest is: {}", Json.encode(r));
                    }

                    if (r.getStatus() != null && !r.getStatus().getUserKafkas().isEmpty()) {
                        return Pair.with(true, r);
                    }
                    return Pair.with(false, null);
                });
        k = await(waitFor(vertx, "ManagedKafkaRequest to complete", ofSeconds(10), ofMinutes(2), ready));
        LOGGER.info("ManagedKafkaRequest is ready: {}", Json.encode(k));
    }


    @Test
    @Order(3)
    void createManagedKafkaConnection(Vertx vertx) {
        assumeTrue(client != null, "the global client is null");

        var managedKafkaRequest = OperatorUtils.managedKafka(client).withName(MANAGED_KAFKA_REQUEST_NAME).get();
        assumeTrue(managedKafkaRequest != null, "the ManagedKafkaRequest is null");
        assumeTrue(managedKafkaRequest.getStatus() != null, "the ManagedKafkaRequest status is null");

        var userKafka = managedKafkaRequest.getStatus().getUserKafkas().stream()
            .filter(k -> k.getName().equals(Environment.LONG_LIVED_KAFKA_NAME))
            .findFirst();

        if (userKafka.isEmpty()) {
            LOGGER.info("ManagedKafkaRequest: {}", Json.encode(managedKafkaRequest));
            fail(String.format("failed to find the user kafka instance %s in the ManagedKafkaRequest %s",
                Environment.LONG_LIVED_KAFKA_NAME, MANAGED_KAFKA_REQUEST_NAME));
        }

        var c = new ManagedKafkaConnection();
        c.getMetadata().setName(MANAGED_KAFKA_CONNECTION_NAME);
        c.setSpec(new ManagedKafkaConnectionSpec());
        c.getSpec().setAccessTokenSecretName(ACCESS_TOKEN_SECRET_NAME);
        c.getSpec().setKafkaId(userKafka.orElseThrow().getId());
        c.getSpec().setCredentials(new Credentials(SERVICE_ACCOUNT_SECRET_NAME));

        LOGGER.info("create ManagedKafkaConnection with name: {}", MANAGED_KAFKA_CONNECTION_NAME);
        c = OperatorUtils.managedKafkaConnection(client).create(c);
        LOGGER.info("created ManagedKafkaConnection: {}", Json.encode(c));

        IsReady<ManagedKafkaConnection> ready = last ->
            Future.succeededFuture(OperatorUtils.managedKafkaConnection(client).withName(MANAGED_KAFKA_CONNECTION_NAME).get())
                .map(r -> {

                    LOGGER.info("ManagedKafkaConnection status is: {}", Json.encode(r.getStatus()));

                    if (last) {
                        LOGGER.warn("last ManagedKafkaConnection is: {}", Json.encode(r));
                    }

                    if (r.getStatus() != null && r.getStatus().getMessage().equals("Created")) {
                        return Pair.with(true, r);
                    }
                    return Pair.with(false, null);
                });
        c = await(waitFor(vertx, "ManagedKafkaConnection to complete", ofSeconds(10), ofMinutes(2), ready));
        LOGGER.info("ManagedKafkaConnection is ready: {}", Json.encode(c));
    }
}
