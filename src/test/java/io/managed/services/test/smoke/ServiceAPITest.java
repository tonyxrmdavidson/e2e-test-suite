package io.managed.services.test.smoke;

import io.managed.services.test.Environment;
import io.managed.services.test.MASOAuth;
import io.managed.services.test.TestBase;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.User;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

    @BeforeAll
    void login(Vertx vertx, VertxTestContext context) {
        this.auth = new MASOAuth(vertx);

        auth.login(Environment.USER_A_USERNAME, Environment.USER_A_PASSWORD)
                .onSuccess(user -> {
                    this.user = user;
                    context.completeNow();
                })
                .onFailure(context::failNow);
    }

    @Test
    void testCreateKafkaInstance(Vertx vertx, VertxTestContext testContext) {
        LOGGER.info("START TEST");
        testContext.completeNow();
    }

}
