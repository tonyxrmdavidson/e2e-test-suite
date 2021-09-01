package io.managed.services.test;

import io.managed.services.test.framework.TestListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Listeners;

@Listeners(TestListener.class)
public abstract class TestBase {
    private static final Logger LOGGER = LogManager.getLogger(TestBase.class);

    protected static final long MINUTES = 60 * 1000;
    protected static final long DEFAULT_TIMEOUT = 3 * MINUTES;

    static {
        LOGGER.info("=======================================================================");
        LOGGER.info("System properties:");
        System.getProperties().forEach((key, value) -> LOGGER.info("{}: {}", key, value));
        LOGGER.info("=======================================================================");
    }
}
