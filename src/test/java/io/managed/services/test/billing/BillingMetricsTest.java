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
        client = new ObservatoriumClient(
                Environment.OBSERVATORIUM_URL,
                Environment.OBSERVATORIUM_CLIENT_ID,
                Environment.OBSERVATORIUM_CLIENT_SECRET,
                Environment.OBSERVATORIUM_OIDC_ISSUER_URL);
        client.start();
    }

    @AfterClass
    public void teardown() {
        client.stop();
    }

    @Test
    public void testBillingMetrics() throws ObservatoriumException {
        QueryResult result = client.query("kafka_id:strimzi_resource_state:max_over_time1h{_id=\"caga1rfmu7m153kcgki0\"}");
        Assert.assertEquals(result.status, "success");
    }
}
