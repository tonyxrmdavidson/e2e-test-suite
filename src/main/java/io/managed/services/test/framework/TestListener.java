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
        LOGGER.info(">> TEST START: {}", result.getName());
    }

    @Override
    public void onTestSuccess(ITestResult result) {
        LOGGER.info("== TEST COMPLETED: {}", result.getName());
    }

    @Override
    public void onTestFailure(ITestResult result) {
        LOGGER.error(result.getThrowable().getMessage());
        LOGGER.error("!! TEST FAILED: {}", result.getName());
    }

    @Override
    public void onTestSkipped(ITestResult result) {
        LOGGER.warn(result.getThrowable().getMessage());
        LOGGER.warn("|| TEST SKIPPED: {}", result.getName());
    }

    @Override
    public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
        LOGGER.warn(result.getThrowable().getMessage());
        LOGGER.warn("?? TEST UNSTABLE: {}", result.getName());
    }

    @Override
    public void onTestFailedWithTimeout(ITestResult result) {
        LOGGER.warn(result.getThrowable().getMessage());
        LOGGER.warn("~~ TEST TIMEOUT: {}", result.getName());
    }

    @Override
    public void onStart(ITestContext context) {
        LOGGER.info("++ SUITE START: {}", context.getClass().getName());
    }

    @Override
    public void onFinish(ITestContext context) {
        LOGGER.info("++ SUITE FINISHED: {}", context.getClass().getName());
    }
}
