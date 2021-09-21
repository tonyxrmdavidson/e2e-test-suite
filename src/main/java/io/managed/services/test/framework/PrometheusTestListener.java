package io.managed.services.test.framework;

import io.managed.services.test.Environment;
import io.prometheus.client.Counter;
import org.testng.ITestListener;
import org.testng.ITestResult;

import static org.testng.ITestResult.FAILURE;
import static org.testng.ITestResult.SKIP;
import static org.testng.ITestResult.SUCCESS;
import static org.testng.ITestResult.SUCCESS_PERCENTAGE_FAILURE;

public class PrometheusTestListener implements ITestListener {

    static final Counter RESULTS = Counter.build()
        .name("test_results")
        .labelNames("launch", "suite", "test", "class", "method", "result")
        .help("Test results counter.")
        .register();

    private void updateResultsMetric(ITestResult result) {
        String res;
        switch (result.getStatus()) {
            case SUCCESS:
                res = "success";
                break;
            case FAILURE:
                res = "failure";
                break;
            case SKIP:
                res = "skip";
                break;
            case SUCCESS_PERCENTAGE_FAILURE:
                res = "unstable";
                break;
            default:
                res = "unknown";
                break;
        }
        var suite = result.getTestContext().getSuite().getName();
        var test = result.getTestContext().getName();
        var clasz = result.getTestClass().getName();
        var method = result.getMethod().getMethodName();
        RESULTS.labels(Environment.KAFKA_POSTFIX_NAME, suite, test, clasz, method, res).inc();
    }


    @Override
    public void onTestSuccess(ITestResult result) {
        updateResultsMetric(result);
    }

    @Override
    public void onTestFailure(ITestResult result) {
        updateResultsMetric(result);
    }

    @Override
    public void onTestSkipped(ITestResult result) {
        updateResultsMetric(result);
    }

    @Override
    public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
        updateResultsMetric(result);
    }

    @Override
    public void onTestFailedWithTimeout(ITestResult result) {
        updateResultsMetric(result);
    }
}
