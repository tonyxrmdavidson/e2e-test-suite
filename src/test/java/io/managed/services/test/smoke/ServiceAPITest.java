package io.managed.services.test.smoke;

import io.managed.services.test.IsReady;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.ext.auth.User;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

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


@Tag(TestTag.SMOKE_SUITE)
@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ServiceAPITest {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPITest.class);

    User user;
    KeycloakOAuth auth;
    String kafkaID;
    ServiceAPI api;

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

        context.completeNow();
    }


    @Test
    @Timeout(5 * 60 * 1000)
    void testCreateKafkaInstance(Vertx vertx, VertxTestContext context) {

        CreateKafkaPayload payload = new CreateKafkaPayload();
        payload.name = "mk-e2e-autotest";
        payload.multiAZ = true;
        payload.cloudProvider = "aws";
        payload.region = "us-east-1";

        LOGGER.info("create kafka instance: {}", payload.name);
        KafkaResponse kafka = await(api.createKafka(payload, true));
        kafkaID = kafka.id;

        IsReady<KafkaResponse> isReady = last -> api.getKafka(kafkaID).map(r -> {
            LOGGER.info("kafka instance status is: {}", r.status);

            if (last) {
                LOGGER.warn("last kafka response is: {}", Json.encode(r));
            }
            return Pair.with(r.status.equals("ready"), r);
        });
        await(waitFor(vertx, "Kafka instance to be ready", ofSeconds(10), ofMinutes(1), isReady));

        context.completeNow();
    }

}
