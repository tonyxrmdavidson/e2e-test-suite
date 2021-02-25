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
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
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

import java.net.HttpURLConnection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.managed.services.test.TestUtils.await;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteKafkaByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteServiceAccountByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsReady;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


@Tag(TestTag.SERVICE_API_PERMISSIONS)
@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ServiceAPISameOrgUserPermissionsTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPISameOrgUserPermissionsTest.class);

    static final String KAFKA_INSTANCE_NAME = "mk-e2e-sup-" + Environment.KAFKA_POSTFIX_NAME;
    static final String SERVICE_ACCOUNT_NAME = "mk-e2e-sup-sa-" + Environment.KAFKA_POSTFIX_NAME;

    ServiceAPI api1;
    ServiceAPI api2;

    KafkaResponse kafka;

    @BeforeAll
    void bootstrap(Vertx vertx) {
        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        api1 = await(ServiceAPIUtils.serviceAPI(vertx, Environment.SSO_USERNAME, Environment.SSO_PASSWORD));

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_SECONDARY_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        api2 = await(ServiceAPIUtils.serviceAPI(vertx, Environment.SSO_SECONDARY_USERNAME, Environment.SSO_SECONDARY_PASSWORD));
    }

    @AfterAll
    void deleteKafkaInstance() {
        await(deleteKafkaByNameIfExists(api1, KAFKA_INSTANCE_NAME));
    }

    @AfterAll
    void deleteServiceAccount() {
        await(deleteServiceAccountByNameIfExists(api1, SERVICE_ACCOUNT_NAME));
    }

    void assertAPI() {
        assumeTrue(api1 != null, "api1 is null because the bootstrap has failed");
        assumeTrue(api2 != null, "api2 is null because the bootstrap has failed");
    }

    void assertKafka() {
        assumeTrue(kafka != null, "kafka is null because the testUser1CreateKafkaInstance has failed to create the Kafka instance");
    }

    @Test
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    @Order(1)
    void testUser1CreateKafkaInstance(Vertx vertx) {
        assertAPI();

        // Create Kafka Instance
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        // add postfix to the name based on owner
        kafkaPayload.name = KAFKA_INSTANCE_NAME;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";

        LOGGER.info("create kafka instance: {}", kafkaPayload.name);
        KafkaResponse k = await(api1.createKafka(kafkaPayload, true));
        kafka = await(waitUntilKafkaIsReady(vertx, api1, k.id));
    }

    @Test
    @Order(2)
    void testUser2ListKafkaInstances() {
        assertKafka();

        // Get kafka instance list by another user with same org
        KafkaListResponse kafkaList = await(api2.getListOfKafkas());
        LOGGER.info("fetch list of kafka instance for another user same org");
        List<KafkaResponse> filteredKafka = kafkaList.items.stream().filter(k -> k.id.equals(kafka.id)).collect(Collectors.toList());

        LOGGER.info("Kafka instance response is: {}", Json.encode(filteredKafka));
        assertEquals(1, filteredKafka.size(), "Kafka is not present for another user with same org");
    }

    @Test
    @Order(2)
    void testUser2ProduceAndConsumeKafkaMessages(Vertx vertx) {
        assertKafka();

        // Create Service Account by another user
        CreateServiceAccountPayload serviceAccountPayload = new CreateServiceAccountPayload();
        serviceAccountPayload.name = SERVICE_ACCOUNT_NAME;

        LOGGER.info("create service account by another user with same org: {}", serviceAccountPayload.name);
        ServiceAccount serviceAccount = await(api2.createServiceAccount(serviceAccountPayload));

        String bootstrapHost = kafka.bootstrapServerHost;
        String clientID = serviceAccount.clientID;
        String clientSecret = serviceAccount.clientSecret;

        // Create Kafka topic by another user
        LOGGER.info("initialize kafka admin; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        KafkaAdmin admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);

        String topicName = "test-topic";
        LOGGER.info("create kafka topic: {}", topicName);
        await(admin.createTopic(topicName));

        KafkaConsumerClient consumer = new KafkaConsumerClient(vertx, topicName, bootstrapHost, clientID, clientSecret);
        KafkaProducerClient producer = new KafkaProducerClient(vertx, topicName, bootstrapHost, clientID, clientSecret);

        //subscribe receiver
        Future<List<KafkaConsumerRecord<String, String>>> received = consumer.receiveAsync(1);

        // Produce Kafka messages
        producer.sendAsync("hello world");

        // Wait for the message
        LOGGER.info("wait for messages");
        List<KafkaConsumerRecord<String, String>> recvMessages = await(received);

        LOGGER.info("Received {} messages", recvMessages.size());
        recvMessages.forEach(record -> assertEquals("hello world", record.value()));
        await(producer.close());
        await(consumer.close());
    }

    @Test
    @Order(3)
    void testUser2DeleteKafkaInstance() {

        // Delete kafka instance by another user with same org
        await(api2.deleteKafka(kafka.id, true)
                .compose(r -> Future.failedFuture("Request should Ideally failed!"))
                .recover(throwable -> {
                    if (throwable instanceof ResponseException) {
                        if (((ResponseException) throwable).response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                            LOGGER.info("another user is not authorised to delete kafka instance");
                            return Future.succeededFuture();
                        }
                    }
                    return Future.failedFuture(throwable);
                }));
    }
}
