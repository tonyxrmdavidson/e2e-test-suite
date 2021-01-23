package io.managed.services.test.smoke;

import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.User;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.function.Supplier;

import static io.managed.services.test.TestUtils.await;
import static io.managed.services.test.TestUtils.waitFor;
import static java.time.Duration.ofSeconds;


@Tag(TestTag.SMOKE_SUITE)
@ExtendWith(VertxExtension.class)
class ServiceAPITest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPITest.class);

    User user;
    KeycloakOAuth auth;
    String kafkaID;
    ServiceAPI api;

    @BeforeAll
    void bootstrap(Vertx vertx, VertxTestContext context) {
        this.auth = new KeycloakOAuth(vertx,
                Environment.SSO_REDHAT_KEYCLOAK_URI,
                Environment.SSO_REDHAT_REDIRECT_URI,
                Environment.SSO_REDHAT_REALM,
                Environment.SSO_REDHAT_CLIENT_ID);

        Future<User> f = auth.login(Environment.SSO_USERNAME, Environment.SSO_PASSWORD);

        f.onSuccess(user -> {
            this.user = user;
            this.api = new ServiceAPI(vertx, Environment.SERVICE_API_URI, user);
        });

        context.assertComplete(f)
                .onComplete(u -> context.completeNow());
    }

    @AfterAll
    void clean(Vertx vertx, VertxTestContext context) {
        if (kafkaID != null) {
            context.assertComplete(api.deleteKafka(kafkaID))
                    .onComplete(u -> context.completeNow());

        } else {
            context.completeNow();
        }
    }


    @Test
    @Timeout(5 * 60 * 1000)
    void testCreateKafkaInstance(Vertx vertx, VertxTestContext context) {

        CreateKafkaPayload payload = new CreateKafkaPayload();
        payload.name = "mk-e2e-autotest";
        payload.multiAZ = true;
        payload.cloudProvider = "aws";
        payload.region = "us-east-1";

        KafkaResponse kafka = await(api.createKafka(payload, true));
        kafkaID = kafka.id;

        Supplier<Future<Boolean>> isReady = () -> api.getKafka(kafkaID).map(r -> r.status.equals("ready"));
        await(waitFor(vertx, "Kafka instance to be ready", ofSeconds(10), ofSeconds(60), isReady));

        context.completeNow();
    }

}
