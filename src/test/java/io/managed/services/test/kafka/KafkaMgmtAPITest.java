package io.managed.services.test.kafka;

import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.TopicSettings;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.KafkaRequestPayload;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import com.openshift.cloud.api.kas.models.ServiceAccountRequest;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.TestUtils;
import io.managed.services.test.ThrowingFunction;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.exception.ApiConflictException;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.kafka.KafkaAuthMethod;
import io.managed.services.test.client.kafka.KafkaProducerClient;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtMetricsUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeoutException;

import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.waitFor;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopic;
import static java.time.Duration.ofSeconds;
import static java.time.Duration.ofMinutes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/**
 * Test Kafka Mgmt API
 *
 * <p>
 * <b>Requires:</b>
 * <ul>
 *     <li> PRIMARY_USERNAME
 *     <li> PRIMARY_PASSWORD
 * </ul>
 */
@Log4j2
public class KafkaMgmtAPITest extends TestBase {

    static final String KAFKA_INSTANCE_NAME = "mk-e2e-" + Environment.LAUNCH_KEY;
    static final String KAFKA2_INSTANCE_NAME = "mk-e2e-2-" + Environment.LAUNCH_KEY;
    static final String SERVICE_ACCOUNT_NAME = "mk-e2e-sa-" + Environment.LAUNCH_KEY;
    static final String TOPIC_NAME = "test-topic";
    static final String METRIC_TOPIC_NAME = "metric-test-topic";
    static final String[] KAFKA_METRICS = {
        "kubelet_volume_stats_available_bytes",
        "kubelet_volume_stats_used_bytes",
        "kafka_broker_quota_softlimitbytes",
        "kafka_broker_quota_totalstorageusedbytes",
        "kafka_server_brokertopicmetrics_messages_in_total",
        "kafka_server_brokertopicmetrics_bytes_in_total",
        "kafka_server_brokertopicmetrics_bytes_out_total",
        "kafka_controller_kafkacontroller_offline_partitions_count",
        "kafka_controller_kafkacontroller_global_partition_count",
        "kafka_topic:kafka_log_log_size:sum",
        "kafka_namespace:haproxy_server_bytes_in_total:rate5m",
        "kafka_namespace:haproxy_server_bytes_out_total:rate5m"
    };

    private KafkaMgmtApi kafkaMgmtApi;
    private SecurityMgmtApi securityMgmtApi;
    private KafkaRequest kafka;
    private ServiceAccount serviceAccount;
    private KafkaInstanceApi kafkaInstanceApi;

    // TODO: Test delete Service Account
    // TODO: Test create existing Service Account

    @BeforeClass
    public void bootstrap() {
        assertNotNull(Environment.PRIMARY_USERNAME, "the PRIMARY_USERNAME env is null");
        assertNotNull(Environment.PRIMARY_PASSWORD, "the PRIMARY_PASSWORD env is null");

        var apps = ApplicationServicesApi.applicationServicesApi(
            Environment.PRIMARY_USERNAME,
            Environment.PRIMARY_PASSWORD);

        securityMgmtApi = apps.securityMgmt();
        kafkaMgmtApi = apps.kafkaMgmt();
    }

    @AfterClass(alwaysRun = true)
    public void teardown() {

        // delete kafka instance
        try {
            KafkaMgmtApiUtils.cleanKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            log.error("clean main kafka instance error: ", t);
        }

        try {
            KafkaMgmtApiUtils.cleanKafkaInstance(kafkaMgmtApi, KAFKA2_INSTANCE_NAME);
        } catch (Throwable t) {
            log.error("clean second kafka instance error: ", t);
        }

        // delete service account
        try {
            SecurityMgmtAPIUtils.cleanServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            log.error("clean service account error: ", t);
        }
    }

    @Test
    @SneakyThrows
    public void testCreateKafkaInstance() {

        // Create Kafka Instance
        var payload = new KafkaRequestPayload()
            .name(KAFKA_INSTANCE_NAME)
            .multiAz(true)
            .cloudProvider("aws")
            .region(Environment.DEFAULT_KAFKA_REGION);

        log.info("create kafka instance '{}'", payload.getName());
        var k = KafkaMgmtApiUtils.createKafkaInstance(kafkaMgmtApi, payload);

        kafka = KafkaMgmtApiUtils.waitUntilKafkaIsReady(kafkaMgmtApi, k.getId());

        kafkaInstanceApi = bwait(KafkaInstanceApiUtils.kafkaInstanceApi(kafka,
            Environment.PRIMARY_USERNAME, Environment.PRIMARY_PASSWORD));
    }

    @Test
    @SneakyThrows
    public void testCreateServiceAccount() {

        // Create Service Account
        log.info("create service account '{}'", SERVICE_ACCOUNT_NAME);
        serviceAccount = securityMgmtApi.createServiceAccount(new ServiceAccountRequest().name(SERVICE_ACCOUNT_NAME));
    }

    @Test(dependsOnMethods = {"testCreateServiceAccount", "testCreateKafkaInstance"})
    @SneakyThrows
    public void testCreateProducerAndConsumerACLs() {

        var principal = KafkaInstanceApiUtils.toPrincipal(serviceAccount.getClientId());
        log.info("create topic and group read and topic write ACLs for the principal '{}'", principal);

        // Create ACLs to consumer and produce messages
        KafkaInstanceApiUtils.createProducerAndConsumerACLs(kafkaInstanceApi, principal);
    }

    @Test(dependsOnMethods = "testCreateKafkaInstance")
    @SneakyThrows
    public void testCreateTopics() {

        var kafkaInstanceApi = bwait(KafkaInstanceApiUtils.kafkaInstanceApi(kafka,
            Environment.PRIMARY_USERNAME, Environment.PRIMARY_PASSWORD));

        log.info("create topic '{}' on the instance '{}'", TOPIC_NAME, kafka.getName());
        var topicPayload = new NewTopicInput()
            .name(TOPIC_NAME)
            .settings(new TopicSettings().numPartitions(1));
        kafkaInstanceApi.createTopic(topicPayload);

        log.info("create topic '{}' on the instance '{}'", METRIC_TOPIC_NAME, kafka.getName());
        var metricTopicPayload = new NewTopicInput()
            .name(METRIC_TOPIC_NAME)
            .settings(new TopicSettings().numPartitions(1));
        kafkaInstanceApi.createTopic(metricTopicPayload);
    }

    @Test(dependsOnMethods = {"testCreateTopics", "testCreateProducerAndConsumerACLs"})
    @SneakyThrows
    public void testMessageInTotalMetric() {

        log.info("test message in total metric");
        KafkaMgmtMetricsUtils.testMessageInTotalMetric(kafkaMgmtApi, kafka, serviceAccount, TOPIC_NAME);
    }

    @Test(dependsOnMethods = {"testCreateTopics", "testCreateProducerAndConsumerACLs"})
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

    @Test(dependsOnMethods = {"testCreateTopics", "testCreateProducerAndConsumerACLs"})
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

    @Test(dependsOnMethods = {"testCreateTopics", "testCreateProducerAndConsumerACLs"})
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

    @Test(dependsOnMethods = {"testCreateTopics", "testCreateProducerAndConsumerACLs"})
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

    @Test(dependsOnMethods = {"testCreateKafkaInstance"})
    @SneakyThrows
    public <T extends Throwable> void testFederateMetrics() {
        // Verify all expected user facing Kafka metrics retrieved from Observatorium are included in the response in a Prometheus Text Format
        var missingMetricsAtom = new AtomicReference<List<String>>();
        ThrowingFunction<Boolean, Boolean, ApiGenericException> isMetricAvailable = last -> {
            var metrics = kafkaMgmtApi.federateMetrics(kafka.getId());
            log.debug(metrics);

            var missingMetrics = new ArrayList<String>();
            for (var metricName : KAFKA_METRICS) {
                var metricTypeDefinition = String.format("# TYPE %s gauge", metricName);
                if (!metrics.contains(metricTypeDefinition)) missingMetrics.add(metricName);
            }

            missingMetricsAtom.set(missingMetrics);
            log.debug("missing metrics: {}", missingMetrics);

            return missingMetrics.size() == 0;
        };
        try {
            waitFor("all federated metrics to become available", ofSeconds(3), ofMinutes(5), isMetricAvailable);
        } catch (TimeoutException e) {
            throw new AssertionError(TestUtils.message("Missing metrics: expected metrics: {} but missing: {}", KAFKA_METRICS, missingMetricsAtom.get()), e);
        }
    }

    @Test(dependsOnMethods = {"testCreateKafkaInstance"})
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
        var kafkaOptional = KafkaMgmtApiUtils.getKafkaByName(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        log.debug(kafkaOptional.orElse(null));
        assertTrue(kafkaOptional.isPresent());
        assertEquals(kafkaOptional.get().getName(), KAFKA_INSTANCE_NAME);
    }

    @Test(dependsOnMethods = {"testCreateKafkaInstance"})
    public void testFailToCreateKafkaInstanceIfItAlreadyExist() {

        // Create Kafka Instance with existing name
        var payload = new KafkaRequestPayload()
            .name(KAFKA_INSTANCE_NAME)
            .multiAz(true)
            .cloudProvider("aws")
            .region(Environment.DEFAULT_KAFKA_REGION);

        log.info("create kafka instance '{}' with existing name", payload.getName());
        assertThrows(ApiConflictException.class, () -> kafkaMgmtApi.createKafka(true, payload));
    }

    @Test(dependsOnMethods = {"testCreateKafkaInstance"}, priority = 1)
    @SneakyThrows
    public void testDeleteKafkaInstance() {

        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccount.getClientId();
        var clientSecret = serviceAccount.getClientSecret();

        // Connect the Kafka producer
        log.info("initialize kafka producer");
        var producer = new KafkaProducerClient<>(
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
        KafkaMgmtApiUtils.waitUntilKafkaIsDeleted(kafkaMgmtApi, kafka.getId());

        // Produce Kafka messages
        log.info("send message to topic '{}'", TOPIC_NAME);
        waitFor(Vertx.vertx(), "sent message to fail", ofSeconds(1), ofSeconds(30), last ->
            producer.send(KafkaProducerRecord.create(TOPIC_NAME, "hello world"))
                .compose(
                    __ -> Future.succeededFuture(Pair.with(false, null)),
                    t -> Future.succeededFuture(Pair.with(true, null))));

        log.info("close kafka producer and consumer");
        bwait(producer.asyncClose());
    }

    @Test(priority = 2)
    @SneakyThrows
    public void testDeleteProvisioningKafkaInstance() {

        // TODO: Move in a regression test class

        // Create Kafka Instance
        var payload = new KafkaRequestPayload()
            .name(KAFKA2_INSTANCE_NAME)
            .multiAz(true)
            .cloudProvider("aws")
            .region(Environment.DEFAULT_KAFKA_REGION);

        log.info("create kafka instance '{}'", KAFKA2_INSTANCE_NAME);
        var kafkaToDelete = KafkaMgmtApiUtils.createKafkaInstance(kafkaMgmtApi, payload);
        log.debug(kafkaToDelete);

        log.info("wait 3 seconds before start deleting");
        Thread.sleep(ofSeconds(3).toMillis());

        log.info("delete kafka '{}'", kafkaToDelete.getId());
        kafkaMgmtApi.deleteKafkaById(kafkaToDelete.getId(), true);

        log.info("wait for kafka to be deleted '{}'", kafkaToDelete.getId());
        KafkaMgmtApiUtils.waitUntilKafkaIsDeleted(kafkaMgmtApi, kafkaToDelete.getId());
    }
}
