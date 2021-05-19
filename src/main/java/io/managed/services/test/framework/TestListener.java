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
        LOGGER.info("-- Start test: {}", result.getName());
    }

    @Override
    public void onTestSuccess(ITestResult result) {
        LOGGER.info("-- Test completed: {}", result.getName());
    }

    @Override
    public void onTestFailure(ITestResult result) {
        LOGGER.info("-- Test failed: {}", result.getName());
    }

    @Override
    public void onTestSkipped(ITestResult result) {
        LOGGER.info("-- Test skipped: {}", result.getName());
    }

    @Override
    public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
        LOGGER.info("-- Test unstable: {}", result.getName());
    }

    @Override
    public void onTestFailedWithTimeout(ITestResult result) {
        LOGGER.info("-- Timeout: {}", result.getName());
    }

    @Override
    public void onStart(ITestContext context) {
        LOGGER.info("== Start suite: {}", context.getName());
    }

    @Override
    public void onFinish(ITestContext context) {
        LOGGER.info("== Suite finished: {}", context.getName());
    }
}
