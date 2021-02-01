package io.managed.services.test.smoke;

import io.managed.services.test.Environment;
import io.managed.services.test.IsReady;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.kafka.KafkaUtils;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAccount;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.CreateServiceAccountPayload;
import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
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
import org.javatuples.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.managed.services.test.TestUtils.await;
import static io.managed.services.test.TestUtils.getKafkaByName;
import static io.managed.services.test.TestUtils.waitFor;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
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


    @Test
    @Timeout(5 * 60 * 1000)
    void testCreateKafkaInstanceIfNotPresent(Vertx vertx, VertxTestContext context) {

        List<Exception> errors = new ArrayList<>();

        // Create Kafka Instance
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        kafkaPayload.name = Environment.LONG_LIVED_KAFKA_NAME;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";

        LOGGER.info("get kafka instance: {}", kafkaPayload.name);
        Optional<KafkaResponse> optionalKafka = await(getKafkaByName(api, kafkaPayload.name));
        KafkaResponse kafkaResponse;
        if (optionalKafka.isEmpty()) {
            LOGGER.error("kafka is not present :{} ", kafkaPayload.name);
            fail(String.format("Something went wrong, kafka is missing .Please create a kafka with name: %s if not created before!", kafkaPayload.name));
        } else {
            kafkaResponse = optionalKafka.get();
            kafkaID = kafkaResponse.id;
            LOGGER.info("kafka is present :{} and created at: {}", kafkaPayload.name, kafkaResponse.createdAt);
        }

        IsReady<KafkaResponse> isReady = last -> api.getKafka(kafkaID).map(r -> {
            LOGGER.info("kafka instance status is: {}", r.status);

            if (last) {
                LOGGER.warn("last kafka response is: {}", Json.encode(r));
            }
            return Pair.with(r.status.equals("ready"), r);
        });

        kafkaResponse = await(waitFor(vertx, "kafka instance to be ready", ofSeconds(10), ofMinutes(5), isReady));

        // Create Service Account
        CreateServiceAccountPayload serviceAccountPayload = new CreateServiceAccountPayload();
        serviceAccountPayload.name = "mk-e2e-autotest";

        LOGGER.info("create service account: {}", serviceAccountPayload.name);
        ServiceAccount serviceAccount = await(api.createServiceAccount(serviceAccountPayload));
        serviceAccountID = serviceAccount.id;

        String bootstrapHost = kafkaResponse.bootstrapServerHost;
        String clientID = serviceAccount.clientID;
        String clientSecret = serviceAccount.clientSecret;

        // Create Kafka topic
        // TODO: User service api to create topics when available
        LOGGER.info("initialize kafka admin; host: {}; clientID: {}; clientSecret: {}", bootstrapHost, clientID, clientSecret);
        KafkaAdmin admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);

        String topicName = "test-topic";
        LOGGER.info("Get kafka topic by name: {}", topicName);
        Map<String, TopicDescription> result = await(admin.getMapOfTopicNameAndDescriptionByName(topicName));
        if (result.containsKey(topicName)) {
            LOGGER.info("Topic is already available for the long live instance");
        } else {
            LOGGER.error("Topic is not present, that mean something went wrong in topic or It is not created earlier!");
            LOGGER.info("Creating new Topic: {}", topicName);
            await(admin.createTopic(topicName));
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
