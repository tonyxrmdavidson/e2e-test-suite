package io.managed.services.test.kafka;

import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.TopicSettings;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.KafkaRequestPayload;
import com.openshift.cloud.api.kas.models.KafkaUpdateRequest;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import com.openshift.cloud.api.kas.models.ServiceAccountRequest;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.ThrowingFunction;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.exception.ApiConflictException;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.kafka.KafkaAdminUtils;
import io.managed.services.test.client.kafka.KafkaAuthMethod;
import io.managed.services.test.client.kafka.KafkaProducerClient;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiAccessUtils;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
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
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.util.HashMap;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.waitFor;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopic;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
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

    static final String SERVICE_ACCOUNT_NAME_FOR_DELETION = "mk-e2e-sa-delete" + Environment.LAUNCH_KEY;
    static final String KAFKA_INSTANCE_NAME = "mk-e2e-" + Environment.LAUNCH_KEY;
    static final String SERVICE_ACCOUNT_NAME = "mk-e2e-sa-" + Environment.LAUNCH_KEY;
    static final String TOPIC_NAME = "test-topic";
    static final String METRIC_TOPIC_NAME = "metric-test-topic";
    static final String[] KAFKA_METRICS = {
        "kafka_server_brokertopicmetrics_messages_in_total",
        "kafka_server_brokertopicmetrics_bytes_in_total",
        "kafka_server_brokertopicmetrics_bytes_out_total",
        "kubelet_volume_stats_available_bytes",
        "kubelet_volume_stats_used_bytes",
        "kafka_broker_quota_softlimitbytes",
        "kafka_broker_quota_totalstorageusedbytes",
        "kafka_controller_kafkacontroller_offline_partitions_count",
        "kafka_controller_kafkacontroller_global_partition_count",
        "kafka_topic:kafka_log_log_size:sum",
        "kafka_namespace:haproxy_server_bytes_in_total:rate5m",
        "kafka_namespace:haproxy_server_bytes_out_total:rate5m",
        "kafka_topic:kafka_topic_partitions:sum",
        "kafka_topic:kafka_topic_partitions:count",
        "consumergroup:kafka_consumergroup_members:count",
        "kafka_namespace:kafka_server_socket_server_metrics_connection_count:sum",
        "kafka_namespace:kafka_server_socket_server_metrics_connection_creation_rate:sum",
        "kafka_topic:kafka_server_brokertopicmetrics_messages_in_total:rate5m",
        "kafka_topic:kafka_server_brokertopicmetrics_bytes_in_total:rate5m",
        "kafka_topic:kafka_server_brokertopicmetrics_bytes_out_total:rate5m"
    };

    private KafkaMgmtApi kafkaMgmtApi;
    private SecurityMgmtApi securityMgmtApi;
    private KafkaRequest kafka;
    private ServiceAccount serviceAccount;
    private KafkaInstanceApi kafkaInstanceApi;


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
        assumeTeardown();

        if (Environment.SKIP_KAFKA_TEARDOWN) {
            try {
                kafkaMgmtApi.updateKafka(kafka.getId(), new KafkaUpdateRequest().reauthenticationEnabled(true));
            } catch (Throwable t) {
                log.warn("resat kafka reauth error: ", t);
            }

            try {
                kafkaInstanceApi.deleteTopic(TOPIC_NAME);
            } catch (Throwable t) {
                log.warn("clean {} topic error: ", TOPIC_NAME, t);
            }

            try {
                kafkaInstanceApi.deleteTopic(METRIC_TOPIC_NAME);
            } catch (Throwable t) {
                log.warn("clean {} topic error: ", METRIC_TOPIC_NAME, t);
            }
        }

        // delete kafka instance
        try {
            KafkaMgmtApiUtils.cleanKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            log.error("clean main kafka instance error: ", t);
        }

        // delete service account
        try {
            SecurityMgmtAPIUtils.cleanServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            log.error("clean service account error: ", t);
        }

        try {
            SecurityMgmtAPIUtils.cleanServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME_FOR_DELETION);
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
            .cloudProvider(Environment.E2E_CLOUD_PROVIDER)
            .region(Environment.DEFAULT_KAFKA_REGION);

        log.info("create kafka instance '{}'", payload.getName());
        kafka = KafkaMgmtApiUtils.applyKafkaInstance(kafkaMgmtApi, payload);

        kafkaInstanceApi = bwait(KafkaInstanceApiUtils.kafkaInstanceApi(kafka,
            Environment.PRIMARY_USERNAME, Environment.PRIMARY_PASSWORD));
    }

    @Test
    @SneakyThrows
    public void testCreateServiceAccount() {

        // Create Service Account
        log.info("create service account '{}'", SERVICE_ACCOUNT_NAME);
        serviceAccount = securityMgmtApi.createServiceAccount(new ServiceAccountRequest()
                .name(SERVICE_ACCOUNT_NAME)
                .description("E2E test service account"));
    }

    @Test(dependsOnMethods = {"testCreateServiceAccount", "testCreateKafkaInstance"})
    @SneakyThrows
    public void testCreateProducerAndConsumerACLs() {

        var principal = KafkaInstanceApiAccessUtils.toPrincipal(serviceAccount.getClientId());
        log.info("create topic and group read and topic write ACLs for the principal '{}'", principal);

        // Create ACLs to consumer and produce messages
        KafkaInstanceApiAccessUtils.createProducerAndConsumerACLs(kafkaInstanceApi, principal);
    }

    @Test(dependsOnMethods = "testCreateKafkaInstance")
    @SneakyThrows
    public void testCreateTopics() {

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
            .cloudProvider(Environment.E2E_CLOUD_PROVIDER)
            .region(Environment.DEFAULT_KAFKA_REGION);

        log.info("create kafka instance '{}' with existing name", payload.getName());
        assertThrows(ApiConflictException.class, () -> kafkaMgmtApi.createKafka(true, payload));
    }

    // TODO change implementation so that there is no endless trying to reauth, (8 times/sec for rest of rests)
    @Ignore
    @Test(dependsOnMethods = {"testCreateServiceAccount", "testCreateKafkaInstance"}, priority = 1)
    @SneakyThrows
    public void testReauthentication() {

        // make sure reauthentication is enabled by default
        assertTrue(kafka.getReauthenticationEnabled());

        var initialSessionLifetimeMs = KafkaAdminUtils.getAuthenticatorPositiveSessionLifetimeMs(
            kafka.getBootstrapServerHost(),
            serviceAccount.getClientId(),
            serviceAccount.getClientSecret());
        log.debug("positiveSessionLifetimeMs: {}", initialSessionLifetimeMs);
        // because reauth is enabled the session lifetime can not be null
        assertNotNull(initialSessionLifetimeMs);

        // disable kafka instance reauthentication
        log.info("set Kafka reauthentication to false");
        kafka = kafkaMgmtApi.updateKafka(kafka.getId(), new KafkaUpdateRequest().reauthenticationEnabled(false));
        assertFalse(kafka.getReauthenticationEnabled());

        ThrowingFunction<Boolean, Boolean, ApiGenericException> isReauthenticationDisabled = last -> {

            var sessionLifetimeMs = KafkaAdminUtils.getAuthenticatorPositiveSessionLifetimeMs(
                kafka.getBootstrapServerHost(),
                serviceAccount.getClientId(),
                serviceAccount.getClientSecret());

            log.debug("positiveSessionLifetimeMs: {}", sessionLifetimeMs);

            // session lifetime should become null after disabling reauth
            return sessionLifetimeMs == null;
        };
        waitFor("kafka reauthentication to be disabled", ofSeconds(10), ofMinutes(5), isReauthenticationDisabled);
    }

    @Test(dependsOnMethods = "testCreateTopics")
    @SneakyThrows
    public void testDeleteServiceAccount() {

        // create SA specifically for purpose of demonstration that it works, afterwards deleting it and fail to use it anymore
        log.info("create service account '{}'", SERVICE_ACCOUNT_NAME_FOR_DELETION);
        var serviceAccountForDeletion = securityMgmtApi.createServiceAccount(new ServiceAccountRequest()
                .name(SERVICE_ACCOUNT_NAME_FOR_DELETION)
                .description("E2E test service account"));

        // ACLs
        var principal = KafkaInstanceApiAccessUtils.toPrincipal(serviceAccountForDeletion.getClientId());
        log.info("create topic and group read and topic write ACLs for the principal '{}'", principal);
        KafkaInstanceApiAccessUtils.createProducerAndConsumerACLs(kafkaInstanceApi, principal);

        // working Communication (Producing & Consuming) using  SA (serviceAccountForDeletion)
        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccountForDeletion.getClientId();
        var clientSecret = serviceAccountForDeletion.getClientSecret();

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

        // deletion of SA (serviceAccountForDeletion)
        securityMgmtApi.deleteServiceAccountById(serviceAccountForDeletion.getId());

        // fail to communicate due to service account being deleted using PLAIN & OAUTH
        assertThrows(KafkaException.class, () -> {
            bwait(testTopic(
                Vertx.vertx(),
                bootstrapHost,
                clientID,
                clientSecret,
                TOPIC_NAME,
                1000,
                10,
                100,
                KafkaAuthMethod.PLAIN
            ));
        });
        assertThrows(KafkaException.class, () -> {
            bwait(testTopic(
                Vertx.vertx(),
                bootstrapHost,
                clientID,
                clientSecret,
                TOPIC_NAME,
                1000,
                10,
                100,
                KafkaAuthMethod.OAUTH
            ));
        });
    }

    @Test(dependsOnMethods = {"testCreateKafkaInstance"}, priority = 2)
    @SneakyThrows
    public void testDeleteKafkaInstance() {
        if (Environment.SKIP_KAFKA_TEARDOWN) {
            throw new SkipException("Skip kafka delete");
        }

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
        waitFor(Vertx.vertx(), "sent message to fail", ofSeconds(3), ofSeconds(30), last ->
            producer.send(KafkaProducerRecord.create(TOPIC_NAME, "hello world"))
                .compose(
                    __ -> Future.succeededFuture(Pair.with(false, null)),
                    t -> Future.succeededFuture(Pair.with(true, null))));

        log.info("close kafka producer and consumer");
        bwait(producer.asyncClose());
    }

}
