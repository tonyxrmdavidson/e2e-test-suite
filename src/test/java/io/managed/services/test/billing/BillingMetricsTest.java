package io.managed.services.test.billing;

import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.observatorium.ObservatoriumClient;
import io.managed.services.test.observatorium.ObservatoriumException;
import io.managed.services.test.observatorium.QueryResult;
import lombok.extern.log4j.Log4j2;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Log4j2
public class BillingMetricsTest extends TestBase {
    private ObservatoriumClient client;

    @BeforeClass
    public void setup() throws ObservatoriumException {
        client = new ObservatoriumClient();
    }

    @AfterClass
    public void teardown() {
    }
}
