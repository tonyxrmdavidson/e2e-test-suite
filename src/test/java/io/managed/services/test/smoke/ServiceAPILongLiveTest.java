package io.managed.services.test.smoke;

import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.kafka.KafkaUtils;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAccount;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.CreateServiceAccountPayload;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.User;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;

import static io.managed.services.test.TestUtils.await;
import static io.managed.services.test.TestUtils.getKafkaByName;
import static io.managed.services.test.TestUtils.waitUntilKafKaGetsReady;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@Tag(TestTag.CI)
@Tag(TestTag.SERVICE_API)
@ExtendWith(VertxExtension.class)
class ServiceAPILongLiveTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPITest.class);

    User user;
    KeycloakOAuth auth;
    ServiceAPI api;

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
        User user = await(auth.login(Environment.SSO_USERNAME, Environment.SSO_PASSWORD));

        this.user = user;
        this.api = new ServiceAPI(vertx, Environment.SERVICE_API_URI, user);

        context.completeNow();
    }

    @AfterAll
    void deleteServiceAccount() {
        if (serviceAccountID != null) {
            LOGGER.info("clean service account: {}", serviceAccountID);
            await(api.deleteServiceAccount(serviceAccountID));
        }
    }

    @Test
    @Timeout(5 * 60 * 1000)
    void testPresenceOfLongLiveKafkaToProduceAndConsumeMessages(Vertx vertx, VertxTestContext context) {

        LOGGER.info("Get kafka instance for name: {}", Environment.LONG_LIVED_KAFKA_NAME);
        Optional<KafkaResponse> optionalKafka = await(getKafkaByName(api, Environment.LONG_LIVED_KAFKA_NAME));
        KafkaResponse kafkaResponse;
        if (optionalKafka.isEmpty()) {
            LOGGER.error("kafka is not present :{} ", Environment.LONG_LIVED_KAFKA_NAME);
            fail(String.format("Something went wrong, kafka is missing. Please create a kafka with name: %s if not created before!", Environment.LONG_LIVED_KAFKA_NAME));
        }
        kafkaResponse = optionalKafka.get();
        kafkaID = kafkaResponse.id;
        LOGGER.info("kafka is present :{} and created at: {}", Environment.LONG_LIVED_KAFKA_NAME, kafkaResponse.createdAt);

        kafkaResponse = waitUntilKafKaGetsReady(vertx, api, kafkaID);

        // Create Service Account
        CreateServiceAccountPayload serviceAccountPayload = new CreateServiceAccountPayload();
        serviceAccountPayload.name = "mk-e2e-autotest";

        LOGGER.info("create service account: {}", serviceAccountPayload.name);
        ServiceAccount serviceAccount = await(api.createServiceAccount(serviceAccountPayload));
        serviceAccountID = serviceAccount.id;

        String bootstrapHost = kafkaResponse.bootstrapServerHost;
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
            LOGGER.error("topic is not present :{} in instance:{} ", Environment.LONG_LIVED_KAFKA_TOPIC_NAME, Environment.LONG_LIVED_KAFKA_NAME);
            fail(String.format("Something went wrong, topic is missing. Please create a topic with name: %s if not created before!", Environment.LONG_LIVED_KAFKA_TOPIC_NAME));
        }

        // Consume Kafka messages
        LOGGER.info("initialize kafka consumer; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        KafkaConsumer<String, String> consumer = KafkaUtils.createConsumer(vertx, bootstrapHost, clientID, clientSecret);

        Promise<KafkaConsumerRecord<String, String>> receiver = Promise.promise();
        consumer.handler(receiver::complete);

        LOGGER.info("subscribe to topic: {}", topicName);
        await(consumer.subscribe(topicName));

        // TODO: Send and receive multiple messages

        // Produce Kafka messages
        LOGGER.info("initialize kafka producer; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        KafkaProducer<String, String> producer = KafkaUtils.createProducer(vertx, bootstrapHost, clientID, clientSecret);

        LOGGER.info("send message to topic: {}", topicName);
        await(producer.send(KafkaProducerRecord.create(topicName, "hello world")));

        // Wait for the message
        LOGGER.info("wait for message");
        KafkaConsumerRecord<String, String> record = await(receiver.future());

        LOGGER.info("received message: {}", record.value());
        assertEquals("hello world", record.value());

        LOGGER.info("close kafka producer and consumer");
        await(producer.close());
        await(consumer.close());

        context.completeNow();

    }

}
