package io.managed.services.test.framework;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

/**
 * jUnit5 specific class which listening on test callbacks
 */
public class TestListener implements ITestListener {
    private static final Logger LOGGER = LogManager.getLogger(TestListener.class);

    @Override
    public void onTestStart(ITestResult result) {
        LOGGER.info("----------------------------");
        LOGGER.info("-- Start test: {}", result.getName());
        LOGGER.info("--");
    }

    @Override
    public void onTestSuccess(ITestResult result) {
        LOGGER.warn("--");
        LOGGER.info("-- Test completed: {}", result.getName());
        LOGGER.warn("----------------------------");
    }

    @Override
    public void onTestFailure(ITestResult result) {
        LOGGER.error(result.getThrowable());
        LOGGER.warn("--");
        LOGGER.error("-- Test failed: {}", result.getName());
        LOGGER.warn("----------------------------");
    }

    @Override
    public void onTestSkipped(ITestResult result) {
        LOGGER.warn(result.getThrowable());
        LOGGER.warn("--");
        LOGGER.warn("-- Test skipped: {}", result.getName());
        LOGGER.warn("----------------------------");
    }

    @Override
    public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
        LOGGER.warn(result.getThrowable());
        LOGGER.warn("--");
        LOGGER.warn("-- Test unstable: {}", result.getName());
        LOGGER.warn("----------------------------");
    }

    @Override
    public void onTestFailedWithTimeout(ITestResult result) {
        LOGGER.warn(result.getThrowable());
        LOGGER.warn("--");
        LOGGER.warn("-- Timeout: {}", result.getName());
        LOGGER.warn("----------------------------");
    }

    @Override
    public void onStart(ITestContext context) {
        LOGGER.info("============================");
        LOGGER.info("== Start suite: {}", context.getName());
        LOGGER.info("==");
    }

    @Override
    public void onFinish(ITestContext context) {
        LOGGER.info("==");
        LOGGER.info("== Suite finished: {}", context.getName());
        LOGGER.info("============================");
    }
}
