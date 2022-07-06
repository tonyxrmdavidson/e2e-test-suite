package io.managed.services.test.kafka;

import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.KafkaRequestPayload;
import io.managed.services.test.Environment;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

@Log4j2
public class StratosphereScenario1 {

    static final String KAFKA_INSTANCE_NAME = "mk-e2e-" + Environment.LAUNCH_KEY;

    private KafkaMgmtApi kafkaMgmtApi;
    private SecurityMgmtApi securityMgmtApi;
    private KafkaRequest kafka;


    @BeforeClass
    public void bootstrap() {
        assertNotNull(Environment.STRATOSPHERE_SCENARIO_1_USER, "the STRATOSPHERE_SCENARIO_1_USER env is null");
        assertNotNull(Environment.STRATOSPHERE_PASSWORD, "the STRATOSPHERE_PASSWORD env is null");

        var apps = ApplicationServicesApi.applicationServicesApi(
                Environment.STRATOSPHERE_SCENARIO_1_USER,
                Environment.STRATOSPHERE_PASSWORD);

        kafkaMgmtApi = apps.kafkaMgmt();
    }

    @AfterClass(alwaysRun = true)
    public void teardown() {
        assumeTeardown();

        // delete kafka instance
        try {
            KafkaMgmtApiUtils.cleanKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            log.error("clean main kafka instance error: ", t);
        }
    }

    /*
    @Test
    @SneakyThrows
    public void testScenario1Case1() {
        // Create Kafka Instance without providing any of the additional parameters (billing_cloud_account_id,
        // billing_model, marketplace)
        var payload = new KafkaRequestPayload()
                .name(KAFKA_INSTANCE_NAME)
                .cloudProvider("aws")
                .region(Environment.DEFAULT_KAFKA_REGION);

        log.info("create kafka instance '{}'", payload.getName());
        kafka = KafkaMgmtApiUtils.applyKafkaInstance(kafkaMgmtApi, payload);

        bwait(KafkaInstanceApiUtils.kafkaInstanceApi(kafka,
                Environment.STRATOSPHERE_SCENARIO_1_USER, Environment.STRATOSPHERE_PASSWORD));
    }
    */

    @Test
    @SneakyThrows
    public void testScenario1Case2() {
        // create Kafka Instance setting the billing model to standard
        // this should fail
        var payload = new KafkaRequestPayload()
                .name(KAFKA_INSTANCE_NAME)
                .cloudProvider("aws")
                .region(Environment.DEFAULT_KAFKA_REGION)
                .billingModel("standard");

        log.info("create kafka instance '{}'", payload.getName());
        assertThrows(ApiGenericException.class, () -> KafkaMgmtApiUtils.applyKafkaInstance(kafkaMgmtApi, payload));
    }
}
