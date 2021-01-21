package io.managed.services.test.smoke;

import io.managed.services.test.Environment;
import io.managed.services.test.OAuth;
import io.managed.services.test.TestBase;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
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


    @BeforeAll
    void login(Vertx vertx, VertxTestContext testContext) {
        OAuth auth = new OAuth(vertx, Environment.USER_A_USERNAME, Environment.USER_A_PASSWORD);
        auth.auth().onSuccess(r -> {
            LOGGER.info(r.bodyAsString());
            LOGGER.info(r.statusCode());
            testContext.completeNow();
        });
    }

    @Test
    void testCreateKafkaInstance(Vertx vertx, VertxTestContext testContext) {
        LOGGER.info("START TEST");
        testContext.completeNow();
    }

}
