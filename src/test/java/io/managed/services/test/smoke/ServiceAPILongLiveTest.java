package io.managed.services.test.smoke;

import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.kafka.KafkaConsumerClient;
import io.managed.services.test.client.kafka.KafkaProducerClient;
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
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.managed.services.test.TestUtils.await;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteServiceAccountByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.getKafkaByName;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(TestTag.CI)
@Tag(TestTag.SERVICE_API)
@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ServiceAPILongLiveTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPITest.class);

    static final String SERVICE_ACCOUNT_NAME = "mk-e2e-ll-sa-" + Environment.KAFKA_POSTFIX_NAME;

    ServiceAPI api;

    KafkaResponse kafka;

    @BeforeAll
    void bootstrap(Vertx vertx) {
        api = await(ServiceAPIUtils.serviceAPI(vertx));
    }

    @AfterAll
    void deleteServiceAccount() {
        await(deleteServiceAccountByNameIfExists(api, SERVICE_ACCOUNT_NAME));
    }

    void assertAPI() {
        assumeTrue(api != null, "api is null because the bootstrap has failed");
    }

    void assertKafka() {
        assumeTrue(kafka != null, "kafka is null because the testPresenceOfLongLiveKafkaInstance has failed to create the Kafka instance");
    }

    @Test
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    @Order(1)
    void testPresenceOfLongLiveKafkaInstance() {
        assertAPI();

        LOGGER.info("Get kafka instance for name: {}", Environment.LONG_LIVED_KAFKA_NAME);
        Optional<KafkaResponse> optionalKafka = await(getKafkaByName(api, Environment.LONG_LIVED_KAFKA_NAME));
        if (optionalKafka.isEmpty()) {
            LOGGER.error("kafka is not present :{} ", Environment.LONG_LIVED_KAFKA_NAME);
            fail(String.format("Something went wrong, kafka is missing. Please create a kafka with name: %s if not created before!", Environment.LONG_LIVED_KAFKA_NAME));
        }

        kafka = optionalKafka.get();
        LOGGER.info("kafka is present :{} and created at: {}", Environment.LONG_LIVED_KAFKA_NAME, kafka.createdAt);
    }

    @Test
    @Order(2)
    void testProduceAndConsumeKafkaMessages(Vertx vertx) {
        assertKafka();

        // Create Service Account
        CreateServiceAccountPayload serviceAccountPayload = new CreateServiceAccountPayload();
        serviceAccountPayload.name = SERVICE_ACCOUNT_NAME;

        LOGGER.info("create service account: {}", serviceAccountPayload.name);
        ServiceAccount serviceAccount = await(api.createServiceAccount(serviceAccountPayload));

        String bootstrapHost = kafka.bootstrapServerHost;
        String clientID = serviceAccount.clientID;
        String clientSecret = serviceAccount.clientSecret;

        LOGGER.info("initialize kafka admin; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        KafkaAdmin admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);

        String topicName = Environment.LONG_LIVED_KAFKA_TOPIC_NAME;
        LOGGER.info("Get kafka topic by name: {}", topicName);

        try {
            Map<String, TopicDescription> result = await(admin.getMapOfTopicNameAndDescriptionByName(topicName));
            if (result.containsKey(topicName)) {
                LOGGER.info("Topic is already available for the long live instance");
            }
        } catch (CompletionException exception) {
            if (exception.getCause() instanceof UnknownTopicOrPartitionException) {
                LOGGER.error("topic is not present: {} in instance: {} ", Environment.LONG_LIVED_KAFKA_TOPIC_NAME, Environment.LONG_LIVED_KAFKA_NAME);
                LOGGER.error("please create the topic with name '{}' in the '{}' instance", Environment.LONG_LIVED_KAFKA_TOPIC_NAME, Environment.LONG_LIVED_KAFKA_NAME);
            }
            fail(exception);
        }

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
        recvMessages.forEach(record -> assertTrue(record.value().contains("hello-world-")));

        LOGGER.info("close kafka producer and consumer");
        await(producer.close());
        await(consumer.close());
    }
}

