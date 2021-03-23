package io.managed.services.test;

import io.managed.services.test.client.ResponseException;
import io.managed.services.test.client.kafkaadminapi.KafkaAdminAPI;
import io.managed.services.test.client.kafkaadminapi.KafkaAdminAPIUtils;
import io.managed.services.test.client.kafkaadminapi.resourcesgroup.CreateTopicPayload;
import io.managed.services.test.client.kafkaadminapi.resourcesgroup.Topic;
import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsReady;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(TestTag.KAFKA_ADMIN_API)
@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KafkaAdminAPITest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPITest.class);

    KafkaAdminAPI kafkaAdminAPI;
    ServiceAPI api;
    String topic;
    String group;


    static final String KAFKA_INSTANCE_NAME = "mk-e2e-kaa-" + Environment.KAFKA_POSTFIX_NAME;
    static final String TEST_TOPIC_NAME = "test-api-topic-1";
    static final String TEST_NOT_EXISTING_TOPIC_NAME = "test-api-topic-notExist";

    static final String TEST_GROUP_NAME = "strimzi-canary-group";
    static final String TEST_NOT_EXISTING_GROUP_NAME = "not-existing-group";


    // TODO: creation of kafka instance may still take more than 5 minutes (what is io.vertx.junit5.VertxTestContext) this problem doesn't occurre

    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    @BeforeAll
    void createKafkaUsingServiceAPI(Vertx vertx, VertxTestContext context)  {
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        kafkaPayload.name = KAFKA_INSTANCE_NAME;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";

        ServiceAPIUtils.serviceAPI(vertx)
                .compose(apiResponse -> {
                    api = apiResponse;
                    return api.createKafka(kafkaPayload, true);
                })
                .compose(kafkaResponse -> waitUntilKafkaIsReady(vertx, api, kafkaResponse.id))
                .compose(kafkaReadyResponse -> {
                    String completeUrl = String.format("https://admin-server-%s", kafkaReadyResponse.bootstrapServerHost);
                    return KafkaAdminAPIUtils.restApi(vertx, completeUrl);
                })
                .onSuccess(restApiResponse -> {
                    System.out.println("kafhka Admin API and stuff sucessfully created");
                    kafkaAdminAPI = restApiResponse;})
                .onFailure(msg -> {
                    System.out.println("not sucesful");
                    System.out.println(msg.getMessage());;
                })
                .onComplete(context.succeedingThenComplete());

        // 14 minutes is the upper bound, execution will continue once Tokens, kafka instance, and connection are created (3-7 minutes approx.). (default: 30sec)
        try {
            context.awaitCompletion(14, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @AfterAll
    void deleteKafkaInstance(VertxTestContext context) {

        ServiceAPIUtils.deleteKafkaByNameIfExists(api, KAFKA_INSTANCE_NAME)
                .onComplete(context.succeedingThenComplete());
    }

    void assertRestAPI() {
        assumeTrue(kafkaAdminAPI != null, "rest API is null because the createKafkaUsingServiceAPI has failed");
    }
    void assertTopic() {
        assumeTrue(topic != null, "topic is null because the testCreateTopic has failed to create the topic on the Kafka instance");
    }

    @Test
    @Order(2)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testCreateTopic(VertxTestContext context)  {
        assertRestAPI();
        CreateTopicPayload topicPayload = setUpTopicPayload();
        kafkaAdminAPI.getSingleTopicByName(TEST_TOPIC_NAME)
                .compose(r -> Future.failedFuture("Getting test-topic should fail in 1st test"))
                .recover(throwable -> {
                    if ((throwable instanceof ResponseException) && (((ResponseException) throwable).response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND)) {
                        LOGGER.info("Topic not found : {}", TEST_TOPIC_NAME);
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })
                .compose(a ->  kafkaAdminAPI.createTopic(topicPayload))
                .onSuccess(createTopicResponse -> context.verify(() -> {
                    assertEquals(TEST_TOPIC_NAME, createTopicResponse.name);
                    topic = createTopicResponse.name;
                }))
                .onComplete(context.succeedingThenComplete());


    }

    @Test
    @Order(3)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testCreateExistingTopic(VertxTestContext context)  {
        assertRestAPI();
        assertTopic();
        CreateTopicPayload topicPayload = setUpTopicPayload();
        kafkaAdminAPI.createTopic(topicPayload)
                .compose(r -> Future.failedFuture("Create existing topic should fail"))
                .recover(throwable -> {
                    if (throwable instanceof ResponseException) {
                        if (((ResponseException) throwable).response.statusCode() == HttpURLConnection.HTTP_CONFLICT) {
                            LOGGER.info("Existing topic cannot be created again : {}", topicPayload.name);
                            return Future.succeededFuture();
                        }
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(3)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testGetTopicByName(VertxTestContext context)  {
        assertRestAPI();
        assertTopic();
        kafkaAdminAPI.getSingleTopicByName(TEST_TOPIC_NAME)
                .onSuccess(topic -> context.verify(() -> assertEquals(TEST_TOPIC_NAME, topic.name)))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(3)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testGetNotExistingTopicByName(VertxTestContext context) {
        assertRestAPI();
        kafkaAdminAPI.getSingleTopicByName(TEST_NOT_EXISTING_TOPIC_NAME)
                .compose(r -> Future.failedFuture("Get none existing topic should fail"))
                .recover(throwable -> {
                    if (throwable instanceof ResponseException) {
                        if (((ResponseException) throwable).response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                            LOGGER.info("Topic not found : {}", TEST_NOT_EXISTING_TOPIC_NAME);
                            return Future.succeededFuture();
                        }
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());

    }


    @Test
    @Order(3)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void tetGetTopics(VertxTestContext context)  {
        assertRestAPI();
        assertTopic();
        kafkaAdminAPI.getAllTopics()
                .onSuccess(topics -> context.verify(() -> {
                    List<Topic> filteredTopics = topics.topics.stream().filter(k -> k.name.equals(TEST_TOPIC_NAME) || k.name.equals("strimzi-canary")).collect(Collectors.toList());
                    assertEquals(2, filteredTopics.size());
                }))
                .onComplete(context.succeedingThenComplete());
    }


    @Test
    @Order(4)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testDeleteTopicByName(VertxTestContext context) {
        assertRestAPI();
        kafkaAdminAPI.deleteTopicByName(TEST_TOPIC_NAME)
                .compose(r -> kafkaAdminAPI.getSingleTopicByName(TEST_TOPIC_NAME))
                .compose(r -> Future.failedFuture("Getting test-topic should fail due to topic being deleted in current test"))
                .recover(throwable -> {
                    if ((throwable instanceof ResponseException) && (((ResponseException) throwable).response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND)) {
                        System.out.println(((ResponseException) throwable).response.bodyAsString());
                        LOGGER.info("Topic not found : {}", TEST_TOPIC_NAME);
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());



    }

    @Test
    @Order(4)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testDeleteNotExistingTopicByName(VertxTestContext context) {
        assertRestAPI();
        kafkaAdminAPI.deleteTopicByName(TEST_NOT_EXISTING_TOPIC_NAME)
                .compose(__ -> Future.failedFuture("Deleting not existing topic should result in HTTP_NOT_FOUND response"))
                .recover(throwable -> {
                    if ((throwable instanceof ResponseException) && (((ResponseException) throwable).response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND)) {
                        LOGGER.info("Topic not found : {}", TEST_NOT_EXISTING_TOPIC_NAME);
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })
                .onComplete(context.succeedingThenComplete());
    }

    // TODO: current API version response to every request with status code 200, thus for now body of response is checked as well.
    @Test
    @Order(2)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testGetGroups(VertxTestContext context) {
        assertRestAPI();
        kafkaAdminAPI.getAllGroups()
                .onSuccess(groupResponse -> context.verify(() -> {
                    int groupsCount =  groupResponse.length;
                    assertTrue(groupsCount >= 1);
                }))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testGetGroupByName(VertxTestContext context) {
        assertRestAPI();
        kafkaAdminAPI.getSingleGroupByName(TEST_GROUP_NAME)
                .onSuccess(groupResponse -> context.verify(() -> {
                    assertEquals(groupResponse.id, TEST_GROUP_NAME);
                    assertEquals("STABLE", groupResponse.state);
                    group = groupResponse.id;
                }))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testGetNotExistingGroupByName(VertxTestContext context) {
        assertRestAPI();
        kafkaAdminAPI.getSingleGroupByName(TEST_NOT_EXISTING_GROUP_NAME)
                .onSuccess(groupResponse -> context.verify(() -> assertEquals("DEAD", groupResponse.state)))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(3)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testDeleteGroupByName(VertxTestContext context) {
        assertRestAPI();
        kafkaAdminAPI.deleteGroupByName(TEST_GROUP_NAME)
                .onSuccess(responseMessage -> context.verify(() -> assertEquals("The group is not empty.", responseMessage)))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testDeleteNotExistingGroupByName(VertxTestContext context) {
        assertRestAPI();
        kafkaAdminAPI.deleteGroupByName(TEST_NOT_EXISTING_GROUP_NAME)
                .onSuccess(responseMessage -> context.verify(() -> assertEquals("The group id does not exist.", responseMessage)))
                .onComplete(context.succeedingThenComplete());
    }


    CreateTopicPayload setUpTopicPayload() {
        LOGGER.info("API: setting up new payload for creation of new Topic using API");
        CreateTopicPayload topicPayload = new CreateTopicPayload();
        topicPayload.name = TEST_TOPIC_NAME;
        topicPayload.settings = new CreateTopicPayload.Settings();
        topicPayload.settings.numPartitions = 3;
        topicPayload.settings.replicationFactor = 3;
        CreateTopicPayload.Settings.Config c1 = new CreateTopicPayload.Settings.Config();
        CreateTopicPayload.Settings.Config c2 = new CreateTopicPayload.Settings.Config();
        c1.key = "min.insync.replicas";
        c1.value = "1";
        c2.key = "max.message.bytes";
        c2.value = "1050000";
        topicPayload.settings.config = new ArrayList<>();
        topicPayload.settings.config.add(c1);
        topicPayload.settings.config.add(c2);
        return topicPayload;
    }

}