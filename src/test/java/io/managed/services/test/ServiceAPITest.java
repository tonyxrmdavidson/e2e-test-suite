package io.managed.services.test;

import io.managed.services.test.client.ResponseException;
import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.kafka.KafkaConsumerClient;
import io.managed.services.test.client.kafka.KafkaProducerClient;
import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.client.serviceapi.CreateServiceAccountPayload;
import io.managed.services.test.client.serviceapi.KafkaListResponse;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.client.serviceapi.ServiceAccount;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.managed.services.test.TestUtils.await;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteKafkaByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteServiceAccountByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.getKafkaByName;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsDeleted;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsReady;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


@Tag(TestTag.CI)
@Tag(TestTag.SERVICE_API)
@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ServiceAPITest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPITest.class);

    static final String KAFKA_INSTANCE_NAME = "mk-e2e-" + Environment.KAFKA_POSTFIX_NAME;
    static final String SERVICE_ACCOUNT_NAME = "mk-e2e-sa-" + Environment.KAFKA_POSTFIX_NAME;
    static final String TOPIC_NAME = "test-topic";

    ServiceAPI api;
    KafkaAdmin admin;

    KafkaResponse kafka;
    ServiceAccount serviceAccount;
    String topic;

    @BeforeAll
    void bootstrap(Vertx vertx) {
        api = await(ServiceAPIUtils.serviceAPI(vertx));
    }

    @AfterAll
    void deleteKafkaInstance() {
        await(deleteKafkaByNameIfExists(api, KAFKA_INSTANCE_NAME));
    }

    @AfterAll
    void deleteServiceAccount() {
        await(deleteServiceAccountByNameIfExists(api, SERVICE_ACCOUNT_NAME));
    }

    void assertAPI() {
        assumeTrue(api != null, "api is null because the bootstrap has failed");
    }

    void assertKafka() {
        assumeTrue(kafka != null, "kafka is null because the testCreateKafkaInstance has failed to create the Kafka instance");
    }

    void assertServiceAccount() {
        assumeTrue(serviceAccount != null, "serviceAccount is null because the testCreateServiceAccount has failed to create the Service Account");
    }

    void assertTopic() {
        assumeTrue(topic != null, "topic is null because the testCreateTopic has failed to create the topic on the Kafka instance");
    }

    /**
     * Create a new Kafka instance
     */
    @Test
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    @Order(1)
    void testCreateKafkaInstance(Vertx vertx) {
        assertAPI();

        // Create Kafka Instance
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        // add postfix to the name based on owner
        kafkaPayload.name = KAFKA_INSTANCE_NAME;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";

        LOGGER.info("create kafka instance: {}", kafkaPayload.name);
        KafkaResponse k = await(api.createKafka(kafkaPayload, true));

        kafka = await(waitUntilKafkaIsReady(vertx, api, k.id));
    }

    @Test
    @Order(1)
    void testCreateServiceAccount() {
        assertAPI();

        // Create Service Account
        CreateServiceAccountPayload serviceAccountPayload = new CreateServiceAccountPayload();
        serviceAccountPayload.name = SERVICE_ACCOUNT_NAME;

        LOGGER.info("create service account: {}", serviceAccountPayload.name);
        serviceAccount = await(api.createServiceAccount(serviceAccountPayload));
    }

    @Test
    @Order(2)
    void testCreateTopic() {
        assertKafka();
        assertServiceAccount();

        var bootstrapHost = kafka.bootstrapServerHost;
        var clientID = serviceAccount.clientID;
        var clientSecret = serviceAccount.clientSecret;

        // Create Kafka topic
        // TODO: User service api to create topics when available
        LOGGER.info("initialize kafka admin; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);

        LOGGER.info("create kafka topic: {}", TOPIC_NAME);
        await(admin.createTopic(TOPIC_NAME));

        topic = TOPIC_NAME;
    }

    @Test
    @Order(3)
    void testProduceAndConsumeKafkaMessages(Vertx vertx) {
        assertTopic();

        var bootstrapHost = kafka.bootstrapServerHost;
        var clientID = serviceAccount.clientID;
        var clientSecret = serviceAccount.clientSecret;

        int msgCount = 1000;
        List<String> messages = IntStream.range(0, msgCount).boxed().map(i -> "hello-world-" + i).collect(Collectors.toList());

        KafkaConsumerClient consumer = new KafkaConsumerClient(vertx, TOPIC_NAME, bootstrapHost, clientID, clientSecret);
        KafkaProducerClient producer = new KafkaProducerClient(vertx, TOPIC_NAME, bootstrapHost, clientID, clientSecret);

        //subscribe receiver
        Future<List<KafkaConsumerRecord<String, String>>> received = consumer.receiveAsync(msgCount);

        // Produce Kafka messages
        List<Future<RecordMetadata>> sentRecords = producer.sendAsync(messages);
        CompositeFuture.all(Arrays.asList(sentRecords.toArray(new Future[0])))
                .onSuccess(compositeFuture -> LOGGER.info("All messages were sent"))
                .onFailure(cause -> fail("Messages were not sent", cause));

        // Wait for the message
        LOGGER.info("wait for messages");
        List<KafkaConsumerRecord<String, String>> recvMessages = await(received);

        LOGGER.info("Received {} messages", recvMessages.size());
        recvMessages.forEach(record -> assertTrue(record.value().contains("hello-world-")));


        LOGGER.info("close kafka producer and consumer");
        await(producer.close());
        await(consumer.close());
    }

    @Test
    @Order(2)
    void testListAndSearchKafkaInstance() {
        assertKafka();

        //List kafka instances
        KafkaListResponse kafkaList = await(api.getListOfKafkas());
        LOGGER.info("fetch kafka instance list: {}", Json.encode(kafkaList.items));
        assertTrue(kafkaList.items.size() > 0);

        //Get created kafka instance from the list
        List<KafkaResponse> filteredKafka = kafkaList.items.stream().filter(k -> k.name.equals(KAFKA_INSTANCE_NAME)).collect(Collectors.toList());
        LOGGER.info("Filter kafka instance from list: {}", Json.encode(filteredKafka));
        assertEquals(1, filteredKafka.size());

        //Search kafka by name
        KafkaResponse kafka = await(getKafkaByName(api, KAFKA_INSTANCE_NAME)).orElseThrow();
        LOGGER.info("Get created kafka instance is : {}", Json.encode(kafka));
        assertEquals(KAFKA_INSTANCE_NAME, kafka.name);
    }

    @Test
    @Order(2)
    void testCreateKafkaInstanceWithExistingName() {
        assertKafka();

        // Create Kafka Instance with existing name
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        kafkaPayload.name = KAFKA_INSTANCE_NAME;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";

        LOGGER.info("create kafka instance with existing name: {}", kafkaPayload.name);
        await(api.createKafka(kafkaPayload, true)
                .compose(r -> Future.failedFuture("create Kafka instance with existing name should fail"))
                .recover(throwable -> {
                    if (throwable instanceof ResponseException) {
                        if (((ResponseException) throwable).response.statusCode() == 409) {
                            LOGGER.info("Existing kafka instance name can't be create : {}", kafkaPayload.name);
                            return Future.succeededFuture();
                        }
                    }
                    return Future.failedFuture(throwable);
                }));
    }

    @Test
    @Order(4)
    void testDeleteTopic() {
        assertTopic();

        LOGGER.info("Delete created topic : {}", TOPIC_NAME);
        await(admin.deleteTopic(TOPIC_NAME));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.MINUTES)
    @Order(5)
    void testDeleteKafkaInstance(Vertx vertx) {
        assertKafka();

        var bootstrapHost = kafka.bootstrapServerHost;
        var clientID = serviceAccount.clientID;
        var clientSecret = serviceAccount.clientSecret;

        // Connect the Kafka producer
        LOGGER.info("initialize kafka producer; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        KafkaProducer<String, String> producer = KafkaProducerClient.createProducer(vertx, bootstrapHost, clientID, clientSecret);

        // Delete the Kafka instance
        LOGGER.info("Delete kafka instance : {}", KAFKA_INSTANCE_NAME);
        await(api.deleteKafka(kafka.id, true));
        await(waitUntilKafkaIsDeleted(vertx, api, kafka.id));

        // Produce Kafka messages
        LOGGER.info("send message to topic: {}", TOPIC_NAME);
        //SslAuthenticationException
        await(producer.send(KafkaProducerRecord.create(TOPIC_NAME, "hello world"))
                .compose(r -> Future.failedFuture("send message should failed"))
                .recover(throwable -> {
                    if (throwable instanceof Exception) {
                        LOGGER.info("send message has failed");
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })
        );

        LOGGER.info("close kafka producer and consumer");
        await(producer.close());
    }
}
