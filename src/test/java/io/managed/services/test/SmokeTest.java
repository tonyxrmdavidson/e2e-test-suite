package io.managed.services.test;

import io.managed.services.test.client.kafka.KafkaMessagingUtils;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.testng.Assert.assertNotNull;


//@Tag(TestTag.SMOKE)
public class SmokeTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(SmokeTest.class);

    @Test
    public void smokeTest() {
        assumeTrue(true);
    }

    @Test
    public void random() {
        var r1 = KafkaMessagingUtils.random(0, 1);
        LOGGER.info("r1: {}", r1);
        assertTrue(r1 >= 0 && r1 <= 1, "r1 is not in range 0,1");

        var r2 = KafkaMessagingUtils.random(10, 1000);
        LOGGER.info("r2: {}", r2);
        assertTrue(r2 >= 10 && r2 <= 1000, "r2 is not in range 10,1000");

        var r3 = KafkaMessagingUtils.random(10, 10);
        LOGGER.info("r3: {}", r3);
        assertEquals(r3, 10, "r3 is not equal 10");
    }

    @Test
    public void testBlockingWait() throws Throwable {
        var vertx = Vertx.vertx();

        var r = new AtomicReference<IllegalCallerException>();
        var p = Promise.promise();
        vertx.setTimer(10, __ -> {
            try {
                TestUtils.bwait(TestUtils.sleep(vertx, Duration.ofSeconds(1)));
                p.complete();
            } catch (IllegalCallerException e) {
                r.set(e);
                p.complete();
            } catch (Throwable throwable) {
                p.fail(throwable);
            }
        });

        // when used outside a vertx callback bwait should succeed
        TestUtils.bwait(p.future());

        // when used within a vertx callback bwait should fail
        assertNotNull(r.get(), "TestUtils.bwait should throw an IllegalCallerException");
    }
}
