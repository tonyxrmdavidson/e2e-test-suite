package io.managed.services.test.registry;


import com.openshift.cloud.api.srs.models.RegistryRest;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.managed.services.test.client.registry.RegistriesApi;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAccount;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.client.registry.RegistriesApiUtils.applyRegistry;
import static io.managed.services.test.client.registry.RegistriesApiUtils.cleanRegistry;
import static io.managed.services.test.client.registry.RegistriesApiUtils.registriesApi;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.applyKafkaInstance;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.applyServiceAccount;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.cleanKafkaInstance;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.cleanServiceAccount;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.serviceAPI;

@Test(groups = TestTag.REGISTRY)
public class RegistryKafkaIntegrationTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(RegistryKafkaIntegrationTest.class);

    private static final String KAFKA_INSTANCE_NAME = "mk-e2e-ki-rki-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String SERVICE_REGISTRY_NAME = "mk-e2e-sr-rki-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String SERVICE_ACCOUNT_NAME = "mk-e2e-sa-rki-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String ARTIFACT_SCHEMA = "{\"type\":\"record\",\"name\":\"Greeting\",\"fields\":[{\"name\":\"Message\",\"type\":\"string\"},{\"name\":\"Time\",\"type\":\"long\"}]}";

    private final Vertx vertx = Vertx.vertx();

    private RegistriesApi registriesApi;
    private RegistryRest registry;
    private ServiceAPI kafkasApi;
    private KafkaResponse kafka;
    private ServiceAccount serviceAccount;

    @BeforeClass(timeOut = 20 * MINUTES)
    public void bootstrap() throws Throwable {

        var oauth = new KeycloakOAuth(vertx);
        var user = bwait(oauth.loginToRHSSO());

        // registry api
        LOGGER.info("initialize registry service api");
        registriesApi = registriesApi(user);

        // kafka api
        LOGGER.info("initialize kafka service api");
        kafkasApi = serviceAPI(vertx, user);

        // registry
        LOGGER.info("create service registry: {}", SERVICE_REGISTRY_NAME);
        registry = applyRegistry(registriesApi, SERVICE_REGISTRY_NAME);

        // kafka
        LOGGER.info("create kafka instance: {}", KAFKA_INSTANCE_NAME);
        kafka = bwait(applyKafkaInstance(vertx, kafkasApi, KAFKA_INSTANCE_NAME));

        // service account
        LOGGER.info("create service account: {}", SERVICE_ACCOUNT_NAME);
        serviceAccount = bwait(applyServiceAccount(kafkasApi, SERVICE_ACCOUNT_NAME));
    }

    @AfterClass(timeOut = DEFAULT_TIMEOUT, alwaysRun = true)
    public void teardown() throws Throwable {
        assumeTeardown();

        try {
            bwait(cleanKafkaInstance(kafkasApi, KAFKA_INSTANCE_NAME));
        } catch (Throwable t) {
            LOGGER.error("clan kafka error: ", t);
        }

        try {
            bwait(cleanServiceAccount(kafkasApi, SERVICE_ACCOUNT_NAME));
        } catch (Throwable t) {
            LOGGER.error("clean service account error: ", t);
        }

        try {
            cleanRegistry(registriesApi, SERVICE_REGISTRY_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean service registry error: ", t);
        }

        bwait(vertx.close());
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void test() {

    }
}
