package io.managed.services.test;

import io.managed.services.test.client.exception.HTTPForbiddenException;
import io.managed.services.test.client.serviceapi.CreateServiceAccountPayload;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.managed.services.test.TestUtils.bwait;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


@Test(groups = TestTag.SERVICE_API)
public class KafkaAPILimitTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(KafkaAPILimitTest.class);

    private static final int SA_LIMIT = 2;
    private static final String SERVICE_ACCOUNT_NAME_PATTERN = "mk-e2e-sa-" + Environment.KAFKA_POSTFIX_NAME;

    private final Vertx vertx = Vertx.vertx();

    private ServiceAPI api;

    @BeforeClass
    public void bootstrap() throws Throwable {
        api = bwait(ServiceAPIUtils.serviceAPI(vertx, Environment.SSO_SECONDARY_USERNAME, Environment.SSO_SECONDARY_PASSWORD));
    }

    private Future<Void> cleanServiceAccounts() {
        return ServiceAPIUtils.deleteServiceAccountsByOwnerIfExists(api, Environment.SSO_SECONDARY_USERNAME);
    }

    @AfterClass
    public void teardown() throws Throwable {
        bwait(cleanServiceAccounts());
    }

    @Test(timeOut = DEFAULT_TIMEOUT, enabled = false)
    public void testLimitServiceAccount() throws Throwable {
        AtomicInteger saSuccessCount = new AtomicInteger(0);

        // Create Service Account payloads
        var payloads = IntStream.range(0, SA_LIMIT + 1).boxed()
            .map(i -> {
                CreateServiceAccountPayload serviceAccountPayload = new CreateServiceAccountPayload();
                serviceAccountPayload.name = SERVICE_ACCOUNT_NAME_PATTERN + "-" + i;
                return serviceAccountPayload;
            })
            .collect(Collectors.toList());

        // remove all SA owned by user
        bwait(cleanServiceAccounts());

        assertThrows(HTTPForbiddenException.class, () -> bwait(TestUtils.forEach(payloads.iterator(), payload ->
            api.createServiceAccount(payload)
                .onSuccess(serviceAccount -> {
                    LOGGER.info("service account {} created", serviceAccount.name);
                    saSuccessCount.incrementAndGet();
                })
                .map(serviceAccount -> null))));

        assertEquals(saSuccessCount.get(), SA_LIMIT, "created service account should equal the limit");
    }
}
