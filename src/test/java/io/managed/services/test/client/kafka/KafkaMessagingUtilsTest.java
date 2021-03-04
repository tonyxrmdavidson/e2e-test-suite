package io.managed.services.test.client.kafka;

import io.managed.services.test.framework.TestTag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(TestTag.SMOKE)
class KafkaMessagingUtilsTest {
    private static final Logger LOGGER = LogManager.getLogger(KafkaMessagingUtilsTest.class);

    @Test
    void random() {
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
}