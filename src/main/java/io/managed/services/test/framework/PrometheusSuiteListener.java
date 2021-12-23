package io.managed.services.test.framework;

import io.managed.services.test.Environment;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;
import lombok.extern.log4j.Log4j2;
import org.testng.ISuite;
import org.testng.ISuiteListener;

import java.io.IOException;
import java.net.URL;

@Log4j2
public class PrometheusSuiteListener implements ISuiteListener {

    @Override
    public void onFinish(ISuite suite) {

        // Push the prometheus metrics collected during the suite execution to the prometheus gateway
        // if it is configured
        if (Environment.PROMETHEUS_PUSH_GATEWAY != null) {
            log.info("push prometheus metrics to: {}", Environment.PROMETHEUS_PUSH_GATEWAY);

            try {
                var pushGateway = new PushGateway(new URL(Environment.PROMETHEUS_PUSH_GATEWAY));
                pushGateway.push(CollectorRegistry.defaultRegistry, "/");
            } catch (IOException e) {
                log.error("failed to push to prometheus:", e);
            }
        }
    }
}
