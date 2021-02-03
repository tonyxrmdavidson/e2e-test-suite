package io.managed.services.test.smoke;

import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.ResponseException;
import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.kafka.KafkaUtils;
import io.managed.services.test.client.oauth.KeycloakOAuth;

import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.client.serviceapi.CreateServiceAccountPayload;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.KafkaListResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAccount;

import io.managed.services.test.framework.TestTag;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.ext.auth.User;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.stream.Collectors;

import static io.managed.services.test.TestUtils.await;
import static io.managed.services.test.TestUtils.waitUntilKafKaGetsReady;

@Tag(TestTag.CI)
@Tag(TestTag.SERVICE_API)
@ExtendWith(VertxExtension.class)
public class ServiceAPISameOrgUserPermissionsTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPISameOrgUserPermissionsTest.class);

    User user1, user2;
    KeycloakOAuth auth;
    ServiceAPI api1, api2;
    String kafkaID;
    String serviceAccountID;

    @BeforeAll
    void bootstrap(Vertx vertx, VertxTestContext context) {
        this.auth = new KeycloakOAuth(vertx,
                Environment.SSO_REDHAT_KEYCLOAK_URI,
                Environment.SSO_REDHAT_REDIRECT_URI,
                Environment.SSO_REDHAT_REALM,
                Environment.SSO_REDHAT_CLIENT_ID);

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        User user1 = await(auth.login(Environment.SSO_USERNAME, Environment.SSO_PASSWORD));

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_SECONDARY_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        User user2 = await(auth.login(Environment.SSO_SECONDARY_USERNAME, Environment.SSO_SECONDARY_PASSWORD));

        this.user1 = user1;
        this.user2 = user2;
        this.api1 = new ServiceAPI(vertx, Environment.SERVICE_API_URI, user1);
        this.api2 = new ServiceAPI(vertx, Environment.SERVICE_API_URI, user2);

        context.completeNow();
    }

    @AfterAll
    void deleteKafkaInstance() {
        if (kafkaID != null) {
            LOGGER.info("clean kafka instance: {}", kafkaID);
            await(api1.deleteKafka(kafkaID, true));
        }
    }

    @AfterAll
    void deleteServiceAccount() {
        if (serviceAccountID != null) {
            LOGGER.info("clean service account: {}", serviceAccountID);
            await(api2.deleteServiceAccount(serviceAccountID));
        }
    }

    @Test
    void testSameOrgUserPermission(Vertx vertx, VertxTestContext context) {
        // Create Kafka Instance
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        // add postfix to the name based on owner
        kafkaPayload.name = Environment.KAFKA_POSTFIX_NAME + "mk-e2e-test";
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";

        LOGGER.info("create kafka instance: {}", kafkaPayload.name);
        KafkaResponse kafka = await(api1.createKafka(kafkaPayload, true));
        kafkaID = kafka.id;
        kafka = waitUntilKafKaGetsReady(vertx, api1, kafkaID);

        // Get kafka instance list by another user with same org
        KafkaListResponse kafkaList = await(api2.getListOfKafkas());
        LOGGER.info("fetch list of kafka instance for another user same org");
        List<KafkaResponse> filteredKafka = kafkaList.items.stream().filter(k -> k.id.equals(kafkaID)).collect(Collectors.toList());

        assertEquals(1, filteredKafka.size(), "Kafka is present for another user with same org");
        LOGGER.info("Kafka instance response is: {}", Json.encode(filteredKafka));

        // Create Service Account by another user
        CreateServiceAccountPayload serviceAccountPayload = new CreateServiceAccountPayload();
        serviceAccountPayload.name = "mk-e2e-autotest";

        LOGGER.info("create service account by another user with same org: {}", serviceAccountPayload.name);
        ServiceAccount serviceAccount = await(api2.createServiceAccount(serviceAccountPayload));
        serviceAccountID = serviceAccount.id;

        String bootstrapHost = kafka.bootstrapServerHost;
        String clientID = serviceAccount.clientID;
        String clientSecret = serviceAccount.clientSecret;

        // Create Kafka topic by another user
        LOGGER.info("initialize kafka admin; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        KafkaAdmin admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);

        String topicName = "test-topic";
        LOGGER.info("create kafka topic: {}", topicName);
        await(admin.createTopic(topicName));

        // Consume Kafka messages
        LOGGER.info("initialize kafka consumer; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        KafkaConsumer<String, String> consumer = KafkaUtils.createConsumer(vertx, bootstrapHost, clientID, clientSecret);

        Promise<KafkaConsumerRecord<String, String>> receiver = Promise.promise();
        consumer.handler(receiver::complete);

        LOGGER.info("subscribe to topic: {}", topicName);
        await(consumer.subscribe(topicName));

        // Produce Kafka messages
        LOGGER.info("initialize kafka producer; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        KafkaProducer<String, String> producer = KafkaUtils.createProducer(vertx, bootstrapHost, clientID, clientSecret);

        LOGGER.info("send message to topic: {}", topicName);
        await(producer.send(KafkaProducerRecord.create(topicName, "test message")));

        // Wait for the message
        LOGGER.info("wait for message");
        KafkaConsumerRecord<String, String> record = await(receiver.future());

        LOGGER.info("received message: {}", record.value());
        assertEquals("test message", record.value());

        LOGGER.info("close kafka producer and consumer");
        await(producer.close());
        await(consumer.close());

        // Delete kafka instance by another user with same org
        await(api2.deleteKafka(kafkaID, true)
                .compose(r -> Future.failedFuture("Request should Ideally failed!"))
                .recover(throwable -> {
                    if (throwable instanceof ResponseException) {
                        if (((ResponseException) throwable).response.statusCode() == 404) {
                            LOGGER.info("another user is not authorised to delete kafka instance");
                            return Future.succeededFuture();
                        }
                    }
                    return Future.failedFuture(throwable);
                }));

        context.completeNow();
    }
}
