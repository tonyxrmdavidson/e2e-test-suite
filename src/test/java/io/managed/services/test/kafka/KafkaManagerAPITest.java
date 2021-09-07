package io.managed.services.test.kafka;

import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.KafkaRequestPayload;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import com.openshift.cloud.api.kas.models.ServiceAccountRequest;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.exception.ApiConflictException;
import io.managed.services.test.client.kafka.KafkaAuthMethod;
import io.managed.services.test.client.kafka.KafkaProducerClient;
import io.managed.services.test.client.kafkaadminapi.KafkaAdminAPIUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtAPIUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtMetricsUtils;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.javatuples.Pair;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;

import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.waitFor;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopic;
import static java.time.Duration.ofSeconds;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


@Test(groups = TestTag.SERVICE_API)
@Log4j2
public class KafkaManagerAPITest extends TestBase {

    static final String KAFKA_INSTANCE_NAME = "mk-e2e-" + Environment.KAFKA_POSTFIX_NAME;
    static final String KAFKA2_INSTANCE_NAME = "mk-e2e-2-" + Environment.KAFKA_POSTFIX_NAME;
    static final String SERVICE_ACCOUNT_NAME = "mk-e2e-sa-" + Environment.KAFKA_POSTFIX_NAME;
    static final String TOPIC_NAME = "test-topic";
    static final String METRIC_TOPIC_NAME = "metric-test-topic";

    private KafkaMgmtApi kafkaMgmtApi;
    private SecurityMgmtApi securityMgmtApi;
    private KafkaRequest kafka;
    private ServiceAccount serviceAccount;

    // TODO: Test delete Service Account
    // TODO: Test create existing Service Account

    @BeforeClass
    public void bootstrap() throws Throwable {
        var auth = new KeycloakOAuth(Environment.SSO_USERNAME, Environment.SSO_PASSWORD);
        var apps = ApplicationServicesApi.applicationServicesApi(auth, Environment.SERVICE_API_URI);

        securityMgmtApi = apps.securityMgmt();
        kafkaMgmtApi = apps.kafkaMgmt();
    }

    @AfterClass(timeOut = DEFAULT_TIMEOUT, alwaysRun = true)
    public void teardown() throws Throwable {

        // delete kafka instance
        try {
            KafkaMgmtAPIUtils.cleanKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            log.error("clean main kafka instance error: ", t);
        }

        try {
            KafkaMgmtAPIUtils.cleanKafkaInstance(kafkaMgmtApi, KAFKA2_INSTANCE_NAME);
        } catch (Throwable t) {
            log.error("clean second kafka instance error: ", t);
        }

        // delete service account
        try {
            SecurityMgmtAPIUtils.deleteServiceAccountByNameIfExists(securityMgmtApi, SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            log.error("clean service account error: ", t);
        }
    }

    @Test(timeOut = 15 * MINUTES)
    @SneakyThrows
    public void testCreateKafkaInstance() {

        // Create Kafka Instance
        var payload = new KafkaRequestPayload()
            .name(KAFKA_INSTANCE_NAME)
            .multiAz(true)
            .cloudProvider("aws")
            .region("us-east-1");

        log.info("create kafka instance '{}'", payload.getName());
        var k = KafkaMgmtAPIUtils.createKafkaInstance(kafkaMgmtApi, payload);

        kafka = KafkaMgmtAPIUtils.waitUntilKafkaIsReady(kafkaMgmtApi, k.getId());
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testCreateServiceAccount() {

        // Create Service Account
        log.info("create service account '{}'", SERVICE_ACCOUNT_NAME);
        serviceAccount = securityMgmtApi.createServiceAccount(new ServiceAccountRequest().name(SERVICE_ACCOUNT_NAME));
    }

    @Test(dependsOnMethods = "testCreateKafkaInstance", timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testCreateTopics() {

        var admin = bwait(KafkaAdminAPIUtils.kafkaAdminAPI(Vertx.vertx(), kafka.getBootstrapServerHost()));

        log.info("create topic '{}' on the instance '{}'", TOPIC_NAME, kafka.getName());
        bwait(KafkaAdminAPIUtils.createDefaultTopic(admin, TOPIC_NAME));

        log.info("create topic '{}' on the instance '{}'", METRIC_TOPIC_NAME, kafka.getName());
        bwait(KafkaAdminAPIUtils.createDefaultTopic(admin, METRIC_TOPIC_NAME));
    }

    @Test(dependsOnMethods = {"testCreateTopics", "testCreateServiceAccount"}, timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testMessageInTotalMetric() {

        log.info("start testing message in total metric");
        KafkaMgmtMetricsUtils.testMessageInTotalMetric(kafkaMgmtApi, kafka, serviceAccount, TOPIC_NAME);
    }

    @Test(dependsOnMethods = {"testCreateTopics", "testCreateServiceAccount"}, timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testMessagingKafkaInstanceUsingOAuth() {

        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccount.getClientId();
        var clientSecret = serviceAccount.getClientSecret();

        bwait(testTopic(
            Vertx.vertx(),
            bootstrapHost,
            clientID,
            clientSecret,
            TOPIC_NAME,
            1000,
            10,
            100,
            KafkaAuthMethod.OAUTH));
    }

    @Test(dependsOnMethods = {"testCreateTopics", "testCreateServiceAccount"}, timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testFailedToMessageKafkaInstanceUsingOAuthAndFakeSecret() {

        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccount.getClientId();

        assertThrows(KafkaException.class, () -> bwait(testTopic(
            Vertx.vertx(),
            bootstrapHost,
            clientID,
            "invalid",
            TOPIC_NAME,
            1,
            10,
            11,
            KafkaAuthMethod.OAUTH)));
    }

    @Test(dependsOnMethods = {"testCreateTopics", "testCreateServiceAccount"}, timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testMessagingKafkaInstanceUsingPlainAuth() {

        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccount.getClientId();
        var clientSecret = serviceAccount.getClientSecret();

        bwait(testTopic(
            Vertx.vertx(),
            bootstrapHost,
            clientID,
            clientSecret,
            TOPIC_NAME,
            1000,
            10,
            100,
            KafkaAuthMethod.PLAIN));
    }

    @Test(dependsOnMethods = {"testCreateTopics", "testCreateServiceAccount"}, timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testFailedToMessageKafkaInstanceUsingPlainAuthAndFakeSecret() {

        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccount.getClientId();

        assertThrows(KafkaException.class, () -> bwait(testTopic(
            Vertx.vertx(),
            bootstrapHost,
            clientID,
            "invalid",
            TOPIC_NAME,
            1,
            10,
            11,
            KafkaAuthMethod.PLAIN)));
    }

    @Test(dependsOnMethods = {"testCreateKafkaInstance"}, timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testListAndSearchKafkaInstance() {

        // TODO: Split in between list kafka and search kafka by name

        // List kafka instances
        log.info("get kafka list");
        var kafkaList = kafkaMgmtApi.getKafkas(null, null, null, null);

        log.debug(kafkaList);
        assertTrue(kafkaList.getItems().size() > 0);

        // Get created kafka instance from the list
        log.info("find kafka instance '{}' in list", KAFKA_INSTANCE_NAME);
        var findKafka = kafkaList.getItems().stream()
            .filter(k -> KAFKA_INSTANCE_NAME.equals(k.getName()))
            .findAny();
        log.debug(findKafka.orElse(null));
        assertTrue(findKafka.isPresent());

        // Search kafka by name
        log.info("search kafka instance '{}' by name", KAFKA_INSTANCE_NAME);
        var kafkaOptional = KafkaMgmtAPIUtils.getKafkaByName(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        log.debug(kafkaOptional.orElse(null));
        assertTrue(kafkaOptional.isPresent());
        assertEquals(kafkaOptional.get().getName(), KAFKA_INSTANCE_NAME);
    }

    @Test(dependsOnMethods = {"testCreateKafkaInstance"}, timeOut = DEFAULT_TIMEOUT)
    public void testFailToCreateKafkaInstanceIfItAlreadyExist() {

        // Create Kafka Instance with existing name
        KafkaRequestPayload payload = new KafkaRequestPayload()
            .name(KAFKA_INSTANCE_NAME)
            .multiAz(true)
            .cloudProvider("aws")
            .region("us-east-1");

        log.info("create kafka instance '{}' with existing name", payload.getName());
        assertThrows(ApiConflictException.class, () -> kafkaMgmtApi.createKafka(true, payload));
    }

    @Test(dependsOnMethods = {"testCreateKafkaInstance"}, priority = 1, timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testDeleteKafkaInstance() {

        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccount.getClientId();
        var clientSecret = serviceAccount.getClientSecret();

        // Connect the Kafka producer
        log.info("initialize kafka producer");
        var producer = KafkaProducerClient.createProducer(
            Vertx.vertx(),
            bootstrapHost,
            clientID,
            clientSecret,
            KafkaAuthMethod.PLAIN,
            StringSerializer.class,
            StringSerializer.class,
            new HashMap<>());

        // Delete the Kafka instance
        log.info("delete kafka instance '{}'", KAFKA_INSTANCE_NAME);
        kafkaMgmtApi.deleteKafkaById(kafka.getId(), true);
        KafkaMgmtAPIUtils.waitUntilKafkaIsDeleted(kafkaMgmtApi, kafka.getId());

        // Produce Kafka messages
        log.info("send message to topic '{}'", TOPIC_NAME);
        waitFor(Vertx.vertx(), "sent message to fail", ofSeconds(1), ofSeconds(30), last ->
            producer.send(KafkaProducerRecord.create(TOPIC_NAME, "hello world"))
                .compose(
                    __ -> Future.succeededFuture(Pair.with(false, null)),
                    t -> Future.succeededFuture(Pair.with(true, null))));

        log.info("close kafka producer and consumer");
        bwait(producer.close());
    }

    @Test(priority = 2, timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testDeleteProvisioningKafkaInstance() {

        // TODO: Move in a regression test class

        // Create Kafka Instance
        KafkaRequestPayload payload = new KafkaRequestPayload()
            .name(KAFKA2_INSTANCE_NAME)
            .multiAz(true)
            .cloudProvider("aws")
            .region("us-east-1");

        log.info("create kafka instance '{}'", KAFKA2_INSTANCE_NAME);
        var kafkaToDelete = KafkaMgmtAPIUtils.createKafkaInstance(kafkaMgmtApi, payload);
        log.debug(kafkaToDelete);

        log.info("wait 3 seconds before start deleting");
        Thread.sleep(ofSeconds(3).toMillis());

        log.info("delete kafka '{}'", kafkaToDelete.getId());
        kafkaMgmtApi.deleteKafkaById(kafkaToDelete.getId(), true);

        log.info("wait for kafka to be deleted '{}'", kafkaToDelete.getId());
        KafkaMgmtAPIUtils.waitUntilKafkaIsDeleted(kafkaMgmtApi, kafkaToDelete.getId());
    }
}
