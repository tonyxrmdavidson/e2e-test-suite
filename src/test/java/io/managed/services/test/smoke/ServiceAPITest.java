package io.managed.services.test.smoke;

import io.managed.services.test.Environment;
import io.managed.services.test.KafkaRequest;
import io.managed.services.test.KafkaRequestPayload;
import io.managed.services.test.MASOAuth;
import io.managed.services.test.ServiceAPI;
import io.managed.services.test.TestBase;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.User;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;


@Tag(TestTag.SMOKE_SUITE)
@ExtendWith(VertxExtension.class)
class ServiceAPITest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPITest.class);

    User user;
    MASOAuth auth;
    String kafkaID;
    ServiceAPI api;

    @BeforeAll
    void login(Vertx vertx, VertxTestContext context) {
        this.auth = new MASOAuth(vertx);

        Future<User> f = auth.login(Environment.USER_A_USERNAME, Environment.USER_A_PASSWORD);

        f.onSuccess(user -> {
            this.user = user;
            this.api = new ServiceAPI(vertx, user);
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
    void testCreateKafkaInstance(Vertx vertx, VertxTestContext context) {

        KafkaRequestPayload payload = new KafkaRequestPayload();
        payload.name = "dbizzarr-autotest";
        Future<KafkaRequest> f = api.createKafka(payload).onSuccess(request -> {
            LOGGER.info(request);
            kafkaID = request.id;
        });

        context.assertComplete(f)
                .onComplete(u -> context.completeNow());
    }

}
