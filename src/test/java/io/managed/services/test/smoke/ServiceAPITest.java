package io.managed.services.test.smoke;

import io.managed.services.test.IsReady;
import io.managed.services.test.KafkaUtils;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.client.serviceapi.CreateServiceAccountPayload;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAccount;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.ext.auth.User;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

import static io.managed.services.test.Environment.SERVICE_API_URI;
import static io.managed.services.test.Environment.SSO_PASSWORD;
import static io.managed.services.test.Environment.SSO_REDHAT_CLIENT_ID;
import static io.managed.services.test.Environment.SSO_REDHAT_KEYCLOAK_URI;
import static io.managed.services.test.Environment.SSO_REDHAT_REALM;
import static io.managed.services.test.Environment.SSO_REDHAT_REDIRECT_URI;
import static io.managed.services.test.Environment.SSO_USERNAME;
import static io.managed.services.test.TestUtils.await;
import static io.managed.services.test.TestUtils.waitFor;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;


@Tag(TestTag.SERVICE_API)
@ExtendWith(VertxExtension.class)
class ServiceAPITest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPITest.class);

    User user;
    KeycloakOAuth auth;
    ServiceAPI api;

    String kafkaID;
    String serviceAccountID;

    @BeforeAll
    void bootstrap(Vertx vertx, VertxTestContext context) {
        this.auth = new KeycloakOAuth(vertx,
                SSO_REDHAT_KEYCLOAK_URI,
                SSO_REDHAT_REDIRECT_URI,
                SSO_REDHAT_REALM,
                SSO_REDHAT_CLIENT_ID);

        LOGGER.info("authenticate user: {} against: {}", SSO_USERNAME, SSO_REDHAT_KEYCLOAK_URI);
        User user = await(auth.login(SSO_USERNAME, SSO_PASSWORD));

        this.user = user;
        this.api = new ServiceAPI(vertx, SERVICE_API_URI, user);

        context.completeNow();
    }

    @AfterAll
    void clean(Vertx vertx, VertxTestContext context) {
        if (kafkaID != null) {
            LOGGER.info("clean kafka instance: {}", kafkaID);
            await(api.deleteKafka(kafkaID));
        }

        if (serviceAccountID != null) {
            LOGGER.info("clean service account: {}", serviceAccountID);
            await(api.deleteServiceAccount(serviceAccountID));
        }

        context.completeNow();
    }


    @Test
    @Timeout(5 * 60 * 1000)
    void testCreateKafkaInstance(Vertx vertx, VertxTestContext context) {

        List<Exception> errors = new ArrayList<>();

        // Create Kafka Instance
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        kafkaPayload.name = "mk-e2e-autotest";
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";

        LOGGER.info("create kafka instance: {}", kafkaPayload.name);
        KafkaResponse kafka = await(api.createKafka(kafkaPayload, true));
        kafkaID = kafka.id;

        IsReady<KafkaResponse> isReady = last -> api.getKafka(kafkaID).map(r -> {
            LOGGER.info("kafka instance status is: {}", r.status);

            if (last) {
                LOGGER.warn("last kafka response is: {}", Json.encode(r));
            }

            boolean ready = r.status.equals("ready");

            // TODO: remove this workaround once the right status is reported
            if (r.bootstrapServerHost != null && !ready) {
                LOGGER.error("not ready kafka response: {}", Json.encode(r));
                errors.add(new Exception("bootstrapServerHost is present but the kafka instance is not ready"));
                return Pair.with(true, r);
            }

            return Pair.with(ready, r);
        });
        kafka = await(waitFor(vertx, "kafka instance to be ready", ofSeconds(10), ofMinutes(5), isReady));

        // Create Service Account
        CreateServiceAccountPayload serviceAccountPayload = new CreateServiceAccountPayload();
        serviceAccountPayload.name = "mk-e2e-autotest";

        LOGGER.info("create service account: {}", serviceAccountPayload.name);
        ServiceAccount serviceAccount = await(api.createServiceAccount(serviceAccountPayload));

        LOGGER.info("service account: {}", Json.encode(serviceAccount));

        // Send Kafka messages
        LOGGER.info("initialize kafka producer; host: {}; clientID: {}; clientSecret: {}",
                kafka.bootstrapServerHost, serviceAccount.clientID, serviceAccount.clientSecret);
        KafkaProducer<String, String> producer = KafkaUtils.createProducer(
                vertx, kafka.bootstrapServerHost, serviceAccount.clientID, serviceAccount.clientSecret);

        LOGGER.info("close kafka producer");
        await(producer.close());

        // TODO: Send and receive messages on one or more topic

        if (!errors.isEmpty()) {
            for (Exception e : errors) {
                LOGGER.error("{}: {}", e.getClass().toString(), e.getMessage());
                e.printStackTrace();
            }
            throw new RuntimeException("test failed with multiple errors");
        }

        context.completeNow();
    }

}
