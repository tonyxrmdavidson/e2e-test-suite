package io.managed.services.test;

import io.managed.services.test.framework.TestTag;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assumptions.assumeTrue;


@Tag(TestTag.SMOKE)
public class SmokeTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(SmokeTest.class);

    @Test
    void smokeTest() {
        assumeTrue(true);
    }

    @Test
    void testBlockingWait() {
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
        Assertions.assertDoesNotThrow(() -> TestUtils.bwait(p.future()));

        // when used within a vertx callback bwait should fail
        Assertions.assertNotNull(r.get(), "TestUtils.bwait should throw an IllegalCallerException");
    }
}
