package io.managed.services.test;

import io.managed.services.test.framework.PrometheusSuiteListener;
import io.managed.services.test.framework.PrometheusTestListener;
import io.managed.services.test.framework.TestListener;
import lombok.extern.log4j.Log4j2;
import org.testng.annotations.Listeners;

@Log4j2
@Listeners({
    TestListener.class,
    PrometheusTestListener.class,
    PrometheusSuiteListener.class})
public abstract class TestBase {

    static {
        log.info("### Environment variables:");
        Environment.getValues().forEach((key, value) -> log.info("{}: {}", key, value));

        log.info("### System properties:");
        System.getProperties().forEach((key, value) -> log.info("{}: {}", key, value));
    }
}
