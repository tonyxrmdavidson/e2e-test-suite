package io.managed.services.test.billing;

import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.Topic;
import com.openshift.cloud.api.kas.auth.models.TopicSettings;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiAccessUtils;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtMetricsUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
import io.managed.services.test.observatorium.ObservatoriumClient;
import io.managed.services.test.observatorium.QueryResult;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopic;
import static org.testng.Assert.assertNotNull;

@Log4j2
public class BillingMetricsTest extends TestBase {

    // TODO (push ready) change name back
    //public static final String KAFKA_INSTANCE_NAME = "mk-billing-" + Environment.LAUNCH_KEY;
    public static final String KAFKA_INSTANCE_NAME = "e";
    public static final String SERVICE_ACCOUNT_NAME = "mk-billing-sa-" + Environment.LAUNCH_KEY;

    // TODO (push ready) change name back
    //public static final String TOPIC_NAME = "billing_test";
    public static final String TOPIC_NAME = "a2";

    private List<Double> myData = new ArrayList<>();

    private ObservatoriumClient observatoriumClient;

    private KafkaRequest kafka;
    private ServiceAccount serviceAccount;

    private KafkaMgmtApi kafkaMgmtApi;
    private SecurityMgmtApi securityMgmtApi;
    private KafkaInstanceApi kafkaInstanceApi;

    private Map<String, Double> snapshotValues = new HashMap<>();

    private static List<String> SNAPSHOT_METRICS;

    private static final String METRIC_STORAGE = "kafka_id:kafka_broker_quota_totalstorageusedbytes:max_over_time1h_gibibyte_months";
    private static final String METRIC_TRAFFIC_TOTAL = "kafka_id:haproxy_server_bytes_in_out_total:rate1h_gibibytes";
    private static final String METRIC_TRAFFIC_IN = "kafka_id:haproxy_server_bytes_in_total:rate1h_gibibytes";
    private static final String METRIC_TRAFFIC_OUT = "kafka_id:haproxy_server_bytes_out_total:rate1h_gibibytes";
    private static final String METRIC_CLUSTER_HOURS = "kafka_id:strimzi_resource_state:max_over_time1h";


    static {
        SNAPSHOT_METRICS = new ArrayList<>();
        SNAPSHOT_METRICS.add(METRIC_TRAFFIC_IN);
        SNAPSHOT_METRICS.add(METRIC_TRAFFIC_OUT);
        SNAPSHOT_METRICS.add(METRIC_TRAFFIC_TOTAL);
        SNAPSHOT_METRICS.add(METRIC_STORAGE);
    }

    // 0.5 Mibibyte
    private final int messageSize = 1024 * 512;
    private final int messageCount = 10;

    @BeforeClass
    @SneakyThrows
    public void setup() {
        assertNotNull(Environment.PRIMARY_USERNAME, "the PRIMARY_USERNAME env is null");
        assertNotNull(Environment.PRIMARY_PASSWORD, "the PRIMARY_PASSWORD env is null");

        var apps = ApplicationServicesApi.applicationServicesApi(
                Environment.PRIMARY_USERNAME,
                Environment.PRIMARY_PASSWORD);

        this.kafkaMgmtApi = apps.kafkaMgmt();
        this.securityMgmtApi = apps.securityMgmt();
        this.observatoriumClient = new ObservatoriumClient();

        // Create Kafka Instance
        var optionalKafkaRequest = KafkaMgmtApiUtils.getKafkaByName(kafkaMgmtApi, KAFKA_INSTANCE_NAME);

        if (optionalKafkaRequest.isPresent()) {
            kafka = optionalKafkaRequest.get();
        } else {
            kafka = KafkaMgmtApiUtils.applyKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        }

        this.kafkaInstanceApi = bwait(KafkaInstanceApiUtils.kafkaInstanceApi(kafka,
                Environment.PRIMARY_USERNAME, Environment.PRIMARY_PASSWORD));

        var optionalServiceAccount = SecurityMgmtAPIUtils.getServiceAccountByName(securityMgmtApi, SERVICE_ACCOUNT_NAME);

        if (optionalServiceAccount.isPresent()) {
            serviceAccount = securityMgmtApi.resetServiceAccountCreds(optionalServiceAccount.get().getId());
        } else {
            serviceAccount = SecurityMgmtAPIUtils.applyServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME);
        }

        createACLs(serviceAccount);
        createTopics();
    }

    @AfterClass
    @SneakyThrows
    public void teardown() {
        // KafkaMgmtApiUtils.deleteKafkaByNameIfExists(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
    }


    @Test(priority = 2)
    @SneakyThrows
    public void testCheckClusterHoursValue() {


        ObservatoriumClient.Query query = new ObservatoriumClient.Query();
        query
                .metric(METRIC_CLUSTER_HOURS)
                .label("_id", kafka.getId());

        QueryResult result = observatoriumClient.query(query);
        Assert.assertEquals(result.status, "success");
        Assert.assertEquals(result.data.result.size(), 1);
        Assert.assertEquals(result.data.result.get(0).value(), "1");
    }

    @Test(priority = 3)
    @SneakyThrows
    public void takeMetricsSnapshot() {
        for (String metric : SNAPSHOT_METRICS) {
            ObservatoriumClient.Query query = new ObservatoriumClient.Query();
            query.metric(metric).label("_id", kafka.getId());
            var result = observatoriumClient.query(query);
            snapshotValues.put(metric, result.data.result.get(0).doubleValue());
        }
    }

    @Test(priority = 4)
    @SneakyThrows
    public void generateLoad() {
        String bootstrapHost = kafka.getBootstrapServerHost();
        String clientID = serviceAccount.getClientId();
        String clientSecret = serviceAccount.getClientSecret();

        log.info("test topic '{}'", TOPIC_NAME);
        bwait(testTopic(Vertx.vertx(),
                bootstrapHost,
                clientID,
                clientSecret,
                TOPIC_NAME,
                this.messageCount, this.messageSize, this.messageSize));
    }

    @Test(priority = 5)
    @SneakyThrows
    public void testStorageIncreased() {

        log.info("test correct storage increase metric when data are produced");
        // storage before increasing (value snapshot created even before data were produced)
        double oldStorageTotal = snapshotValues.get(METRIC_STORAGE);

        // calculation of expected increased value in metric, i.e, conversion of produced bytes (messages * size) to Gibibytes (1024^3), normalized by metricShift (10^3).
        double expectedIncrease = this.messageSize * this.messageCount * 4 / (Math.pow(1024.0, 3.0)) / 1000;

        // waiting for metric to be increased within with 5 range
        KafkaMgmtMetricsUtils.waitUntilExpectedMetricRange(
                observatoriumClient,
                kafka.getId(),
                METRIC_STORAGE,
                oldStorageTotal,
                expectedIncrease,
                5.0);
    }

    @SneakyThrows
    private void createTopics() {
        var topics = kafkaInstanceApi.getTopics();
        for (Topic topic : topics.getItems()) {
            if (topic.getName().equals(TOPIC_NAME)) {
                return;
            }
        }

        // create topic with 3 replicas (the only possible way to create topic currently)
        kafkaInstanceApi.createTopic(
                new NewTopicInput()
                        .name(TOPIC_NAME)
                        .settings(
                                new TopicSettings()
                                        .numPartitions(1)));
    }

    private void createACLs(ServiceAccount serviceAccount) throws ApiGenericException {
        if (serviceAccount != null) {
            var principal = KafkaInstanceApiAccessUtils.toPrincipal(serviceAccount.getClientId());
            KafkaInstanceApiAccessUtils.createProducerAndConsumerACLs(kafkaInstanceApi, principal);
        }
    }
}
