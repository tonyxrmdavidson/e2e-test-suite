package io.managed.services.test.kafka;


import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.TopicSettings;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.kafka.KafkaAuthMethod;
import io.managed.services.test.client.kafka.KafkaConsumerClient;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtMetricsUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.message;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopic;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopicWithMultipleConsumers;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * Test a Long Live Kafka Instance that will be deleted only before it expires, but otherwise it will continue to run
 * also after the tests are concluded. This is a very basic test to ensure that an update doesn't break
 * an existing Kafka Instance.
 */
@Test(groups = TestTag.SERVICE_API)
public class LongLiveKafkaInstanceTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(LongLiveKafkaInstanceTest.class);

    public static final String KAFKA_INSTANCE_NAME = "mk-e2e-ll-" + Environment.KAFKA_POSTFIX_NAME;
    public static final String SERVICE_ACCOUNT_NAME = "mk-e2e-ll-sa-" + Environment.KAFKA_POSTFIX_NAME;

    private static final String TEST_TOPIC_NAME = "test-topic";
    private static final String MULTI_PARTITION_TOPIC_NAME = "multi-partitions-topic";
    private static final String METRIC_TOPIC_NAME = "metric-test-topic";

    static final String TEST_CANARY_NAME = "__strimzi_canary";
    public static final String TEST_CANARY_GROUP = "canary-group";

    private KafkaMgmtApi kafkaMgmtApi;
    private SecurityMgmtApi securityMgmtApi;
    private KafkaInstanceApi kafkaInstanceApi;

    private KafkaRequest kafka;
    private ServiceAccount serviceAccount;

    @BeforeClass
    public void bootstrap() {
        var apps = ApplicationServicesApi.applicationServicesApi(Environment.SERVICE_API_URI,
            Environment.SSO_USERNAME, Environment.SSO_PASSWORD);

        this.kafkaMgmtApi = apps.kafkaMgmt();
        this.securityMgmtApi = apps.securityMgmt();
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testThatTheLongLiveKafkaInstanceAlreadyExist() {

        LOGGER.info("get kafka instance '{}'", KAFKA_INSTANCE_NAME);
        var optionalKafka = KafkaMgmtApiUtils.getKafkaByName(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        if (optionalKafka.isEmpty()) {
            fail(message("the long living kafka instance '{}' doesn't exist", KAFKA_INSTANCE_NAME));
        }
        kafka = optionalKafka.get();
        LOGGER.debug(kafka);
    }

    @Test(priority = 1, timeOut = 15 * MINUTES)
    @SneakyThrows
    public void testRecreateTheLongLiveKafkaInstanceIfItDoesNotExist() {
        // recreate the instance only if it doesn't exist
        if (Objects.isNull(kafka)) {
            LOGGER.info("create kafka instance '{}'", KAFKA_INSTANCE_NAME);
            kafka = KafkaMgmtApiUtils.applyKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        }

        LOGGER.info("initialize kafka instance api");
        kafkaInstanceApi = bwait(KafkaInstanceApiUtils.kafkaInstanceApi(kafka,
            Environment.SSO_USERNAME, Environment.SSO_PASSWORD));
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testThatTheLongLiveServiceAccountAlreadyExist() {

        LOGGER.info("get service account '{}'", SERVICE_ACCOUNT_NAME);
        var optionalAccount = SecurityMgmtAPIUtils.getServiceAccountByName(securityMgmtApi, SERVICE_ACCOUNT_NAME);
        if (optionalAccount.isEmpty()) {
            fail(message("the long living service account with name '{}' doesn't exist", SERVICE_ACCOUNT_NAME));
        }

        LOGGER.info("reset credentials for service account '{}'", SERVICE_ACCOUNT_NAME);
        serviceAccount = securityMgmtApi.resetServiceAccountCreds(optionalAccount.get().getId());
    }


    @Test(priority = 1, timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testRecreateTheLongLiveServiceAccountIfItDoesNotExist() {
        // recreate the account only if it doesn't exist
        if (Objects.isNull(serviceAccount)) {
            LOGGER.info("create service account '{}'", SERVICE_ACCOUNT_NAME);
            serviceAccount = SecurityMgmtAPIUtils.applyServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME);
        }
    }

    private Map<String, NewTopicInput> expectedTopics() {
        BiFunction<String, Integer, NewTopicInput> topic = (String topicName, Integer numPartitions) ->
            new NewTopicInput()
                .name(topicName)
                .settings(new TopicSettings().numPartitions(numPartitions));

        var map = new HashMap<String, NewTopicInput>();
        map.put(TEST_TOPIC_NAME, topic.apply(TEST_TOPIC_NAME, 1));
        map.put(METRIC_TOPIC_NAME, topic.apply(METRIC_TOPIC_NAME, 1));
        map.put(MULTI_PARTITION_TOPIC_NAME, topic.apply(MULTI_PARTITION_TOPIC_NAME, 3));
        return map;
    }

    @Test(dependsOnMethods = "testRecreateTheLongLiveKafkaInstanceIfItDoesNotExist", timeOut = DEFAULT_TIMEOUT)
    public void testThatAllLongLiveTopicsAlreadyExist() throws Throwable {

        final var expectedTopics = expectedTopics();

        final var topicList = kafkaInstanceApi.getTopics();
        Objects.requireNonNull(topicList.getItems()).forEach(t -> expectedTopics.remove(t.getName()));

        assertTrue(expectedTopics.isEmpty(), message("the topics '{}' don't exist", expectedTopics.keySet()));
    }

    @Test(priority = 2, dependsOnMethods = "testRecreateTheLongLiveKafkaInstanceIfItDoesNotExist", timeOut = DEFAULT_TIMEOUT)
    @SneakyThrows
    public void testRecreateLongLiveTopicsIfTheyDoNotExist() {

        final var expectedTopics = expectedTopics();

        final var topicList = kafkaInstanceApi.getTopics();
        final var unexpectedTopics = Objects.requireNonNull(topicList.getItems())
            .stream()
            .filter(t -> Objects.isNull(expectedTopics.remove(t.getName())))
            .collect(Collectors.toList());

        for (var topic : unexpectedTopics) {
            LOGGER.info("delete unexpected topic '{}'", topic.getName());
            kafkaInstanceApi.deleteTopic(topic.getName());
        }

        for (var topic : expectedTopics.values()) {
            LOGGER.info("create expected topic '{}'", topic.getName());
            kafkaInstanceApi.createTopic(topic);
        }
    }


    @Test(dependsOnMethods = {
        "testRecreateTheLongLiveKafkaInstanceIfItDoesNotExist",
        "testRecreateTheLongLiveServiceAccountIfItDoesNotExist"
    }, timeOut = DEFAULT_TIMEOUT)
    void testThatTheCanaryTopicExist() {

        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccount.getClientId();
        var clientSecret = serviceAccount.getClientSecret();

        var admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);
        var topics = admin.listTopics();
        LOGGER.debug(topics);

        var topicOptional = topics.stream().filter(e -> e.equals(TEST_CANARY_NAME)).findAny();
        assertTrue(topicOptional.isPresent());
    }


    @Test(dependsOnMethods = "testThatTheCanaryTopicExist", timeOut = DEFAULT_TIMEOUT)
    void testCanaryLiveliness() throws Throwable {

        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccount.getClientId();
        var secret = serviceAccount.getClientSecret();

        var consumerClient = new KafkaConsumerClient<>(
            Vertx.vertx(),
            bootstrapHost,
            clientID, secret,
            KafkaAuthMethod.OAUTH,
            TEST_CANARY_GROUP,
            "latest",
            StringDeserializer.class,
            StringDeserializer.class);
        var asyncRecords = bwait(consumerClient.receiveAsync(TEST_CANARY_NAME, 1));

        LOGGER.info("wait for records");
        var records = bwait(asyncRecords);
        LOGGER.debug(records);
    }


    @Test(dependsOnMethods = {
        "testRecreateTheLongLiveKafkaInstanceIfItDoesNotExist",
        "testRecreateLongLiveTopicsIfTheyDoNotExist",
        "testRecreateTheLongLiveServiceAccountIfItDoesNotExist"
    }, timeOut = DEFAULT_TIMEOUT)
    public void testProduceAndConsumeKafkaMessages() throws Throwable {

        String bootstrapHost = kafka.getBootstrapServerHost();
        String clientID = serviceAccount.getClientId();
        String clientSecret = serviceAccount.getClientSecret();

        LOGGER.info("test topic '{}'", TEST_TOPIC_NAME);
        bwait(testTopic(Vertx.vertx(),
            bootstrapHost,
            clientID,
            clientSecret,
            TEST_TOPIC_NAME,
            10,
            7,
            10));
    }

    @Test(dependsOnMethods = {
        "testRecreateTheLongLiveKafkaInstanceIfItDoesNotExist",
        "testRecreateLongLiveTopicsIfTheyDoNotExist",
        "testRecreateTheLongLiveServiceAccountIfItDoesNotExist"
    }, timeOut = DEFAULT_TIMEOUT)
    void testTopicWithThreePartitionsAndThreeConsumers() throws Throwable {

        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccount.getClientId();
        var clientSecret = serviceAccount.getClientSecret();

        LOGGER.info("test topic '{}' with 3 consumers", MULTI_PARTITION_TOPIC_NAME);
        bwait(testTopicWithMultipleConsumers(Vertx.vertx(),
            bootstrapHost,
            clientID,
            clientSecret,
            MULTI_PARTITION_TOPIC_NAME,
            Duration.ofMinutes(1),
            1000,
            99,
            100,
            3));
    }

    @Test(dependsOnMethods = {
        "testRecreateTheLongLiveKafkaInstanceIfItDoesNotExist",
        "testRecreateLongLiveTopicsIfTheyDoNotExist",
        "testRecreateTheLongLiveServiceAccountIfItDoesNotExist"
    }, timeOut = DEFAULT_TIMEOUT)
    public void testMessageInTotalMetric() throws Throwable {

        LOGGER.info("test message in total metric");
        KafkaMgmtMetricsUtils.testMessageInTotalMetric(kafkaMgmtApi, kafka, serviceAccount, METRIC_TOPIC_NAME);
    }
}

