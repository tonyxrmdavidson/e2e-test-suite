package io.managed.services.test;

import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.kafka.KafkaConsumerClient;
import io.managed.services.test.client.kafka.KafkaProducerClient;
import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.client.serviceapi.CreateServiceAccountPayload;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.client.serviceapi.ServiceAccount;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.managed.services.test.TestUtils.await;
import static io.managed.services.test.client.kafka.KafkaUtils.getTopicByName;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.getKafkaByName;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.getServiceAccountByName;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsReady;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


@Tag(TestTag.SERVICE_API)
@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ServiceAPILongLiveTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPITest.class);

    static final String KAFKA_INSTANCE_NAME = "mk-e2e-ll-" + Environment.KAFKA_POSTFIX_NAME;
    static final String SERVICE_ACCOUNT_NAME = "mk-e2e-ll-sa-" + Environment.KAFKA_POSTFIX_NAME;
    static final String TOPIC_NAME = "ll-test-topic";

    ServiceAPI api;

    KafkaResponse kafka;
    ServiceAccount serviceAccount;
    String topic;

    @BeforeAll
    void bootstrap(Vertx vertx) {
        api = await(ServiceAPIUtils.serviceAPI(vertx));
    }

    void assertAPI() {
        assumeTrue(api != null, "api is null because the bootstrap has failed");
    }

    void assertKafka() {
        assumeTrue(kafka != null, "kafka is null because the testPresenceOfLongLiveKafkaInstance has failed to create the Kafka instance");
    }

    void assertServiceAccount() {
        assumeTrue(serviceAccount != null, "serviceAccount is null because the testPresenceOfTheServiceAccount has failed to create the Service Account");
    }

    void assertTopic() {
        assumeTrue(topic != null, "topic is null because the testPresenceOfTopic has failed to create the Topic");
    }

    @Test
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    @Order(1)
    void testPresenceOfLongLiveKafkaInstance(Vertx vertx) {
        assertAPI();

        LOGGER.info("get kafka instance for name: {}", KAFKA_INSTANCE_NAME);
        var optionalKafka = await(getKafkaByName(api, KAFKA_INSTANCE_NAME));
        if (optionalKafka.isEmpty()) {
            LOGGER.error("kafka is not present: {}", KAFKA_INSTANCE_NAME);

            LOGGER.info("try to recreate the kafka instance: {}", KAFKA_INSTANCE_NAME);
            // Create Kafka Instance
            var kafkaPayload = new CreateKafkaPayload();
            kafkaPayload.name = KAFKA_INSTANCE_NAME;
            kafkaPayload.multiAZ = true;
            kafkaPayload.cloudProvider = "aws";
            kafkaPayload.region = "us-east-1";

            LOGGER.info("create kafka instance: {}", kafkaPayload.name);
            var k = await(api.createKafka(kafkaPayload, true));
            kafka = await(waitUntilKafkaIsReady(vertx, api, k.id));

            fail(String.format("for some reason the long living kafka instance with name: %s didn't exists anymore but we have recreate it", KAFKA_INSTANCE_NAME));
        }

        kafka = optionalKafka.get();
        LOGGER.info("kafka is present :{} and created at: {}", KAFKA_INSTANCE_NAME, kafka.createdAt);
    }

    @Test
    @Order(2)
    void testPresenceOfServiceAccount() {
        assertKafka();

        LOGGER.info("get service account by name: {}", SERVICE_ACCOUNT_NAME);
        var optionalAccount = await(getServiceAccountByName(api, SERVICE_ACCOUNT_NAME));
        if (optionalAccount.isEmpty()) {
            LOGGER.error("service account is not present: {}", SERVICE_ACCOUNT_NAME);

            LOGGER.info("try to recreate the service account: {}", SERVICE_ACCOUNT_NAME);
            // Create Service Account
            var serviceAccountPayload = new CreateServiceAccountPayload();
            serviceAccountPayload.name = SERVICE_ACCOUNT_NAME;

            LOGGER.info("create service account: {}", serviceAccountPayload.name);
            serviceAccount = await(api.createServiceAccount(serviceAccountPayload));

            fail(String.format("for some reason the long living service account with name: %s didn't exists anymore but we have recreate it", SERVICE_ACCOUNT_NAME));
        }

        LOGGER.info("reset credentials for service account: {}", SERVICE_ACCOUNT_NAME);
        serviceAccount = await(api.resetCredentialsServiceAccount(optionalAccount.get().id));
    }

    @Test
    @Order(3)
    void testPresenceOfTopic() {
        assertServiceAccount();

        String bootstrapHost = kafka.bootstrapServerHost;
        String clientID = serviceAccount.clientID;
        String clientSecret = serviceAccount.clientSecret;

        LOGGER.info("initialize kafka admin; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        var admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);

        LOGGER.info("get the topic by name: {}", TOPIC_NAME);
        var optionalTopic = await(getTopicByName(admin, TOPIC_NAME));

        if (optionalTopic.isEmpty()) {
            LOGGER.error("topic is not present: {}", TOPIC_NAME);

            LOGGER.info("try to recreate the topic: {}", TOPIC_NAME);
            await(admin.createTopic(TOPIC_NAME));

            topic = TOPIC_NAME;

            fail(String.format("for some reason the long living topic: %s in the kafka instance: %s didn't exists anymore but we have recreate it",
                    KAFKA_INSTANCE_NAME, TOPIC_NAME));
        }

        LOGGER.info("topic is present :{}", TOPIC_NAME);
        topic = TOPIC_NAME;
    }

    @Test
    @Order(4)
    void testProduceAndConsumeKafkaMessages(Vertx vertx) {
        assertTopic();

        String bootstrapHost = kafka.bootstrapServerHost;
        String clientID = serviceAccount.clientID;
        String clientSecret = serviceAccount.clientSecret;
        String topicName = topic;

        int msgCount = 1000;
        List<String> messages = IntStream.range(0, msgCount).boxed().map(i -> "hello-world-" + i).collect(Collectors.toList());

        KafkaConsumerClient consumer = new KafkaConsumerClient(vertx, topicName, bootstrapHost, clientID, clientSecret);
        KafkaProducerClient producer = new KafkaProducerClient(vertx, topicName, bootstrapHost, clientID, clientSecret);

        //subscribe receiver
        Future<List<KafkaConsumerRecord<String, String>>> received = consumer.receiveAsync(msgCount);

        // Produce Kafka messages
        producer.sendAsync(messages);

        // Wait for the message
        LOGGER.info("wait for messages");
        List<KafkaConsumerRecord<String, String>> recvMessages = await(received);

        LOGGER.info("Received {} messages", recvMessages.size());
        assertEquals(msgCount, recvMessages.size());

        LOGGER.info("close kafka producer and consumer");
        await(producer.close());
        await(consumer.close());
    }
}

