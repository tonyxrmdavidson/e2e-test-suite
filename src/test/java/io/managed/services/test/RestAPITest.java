package io.managed.services.test;

import io.managed.services.test.client.restapi.RestAPI;
import io.managed.services.test.client.restapi.RestAPIUtils;
import io.managed.services.test.client.restapi.resources.CreateTopicPayload;
import io.managed.services.test.client.restapi.resources.Topic;
import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

//import static io.managed.services.test.TestUtils.await;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteKafkaByNameIfExists;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(TestTag.REST_API)
@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RestAPITest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPITest.class);

    RestAPI restAPI;
    ServiceAPI api;
    String topic ;
    String group;


    static final String KAFKA_INSTANCE_NAME = "mk-e22e-" + Environment.KAFKA_POSTFIX_NAME;
    static final String TEST_TOPIC_NAME = "test-api-topic-1";
    static final String TEST_NOT_EXISTING_TOPIC_NAME = "test-api-topic-notExist";

    static final String TEST_GROUP_NAME = "strimzi-canary-group";
    static final String TEST_NOT_EXISTING_GROUP_NAME = "not-existing-group";

    void waitingForRestAPi(Vertx vertx, String bHost, VertxTestContext context) {
        String completeUrl = "https://admin-server-".concat(bHost);
        RestAPIUtils.restApi(vertx, completeUrl)
                .onComplete( restApiResponse -> {
                    restAPI = restApiResponse.result();
                    context.completeNow();
                });
    }


    void waitingForKafkaReady(Vertx vertx, KafkaResponse kafkaInstance, VertxTestContext context) {
        ServiceAPIUtils.waitUntilKafkaIsReady(vertx, api, kafkaInstance.id)
                .onComplete( createdKafkaInstanceResponse -> waitingForRestAPi(vertx , createdKafkaInstanceResponse.result().bootstrapServerHost, context));
    }

    void waitingForKafkaCreated(Vertx vertx, ServiceAPI apiResponded, VertxTestContext context){
        CreateKafkaPayload kafkaPayload = new CreateKafkaPayload();
        kafkaPayload.name = KAFKA_INSTANCE_NAME;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";

        apiResponded.createKafka(kafkaPayload, true)
                .onComplete( (kResp) -> waitingForKafkaReady(vertx , kResp.result(), context));
    }

    @BeforeAll
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void createKafkaUsingServiceAPI(Vertx vertx, VertxTestContext context) throws InterruptedException {
        ServiceAPIUtils.serviceAPI(vertx)
                .onComplete( a -> {
                    api = a.result();
                    waitingForKafkaCreated(vertx, a.result(), context);
                });

        // 8 minutes is upper bound, execution will continue once Tokens, kafka instance, and connection to it is created (3-6 minx)
        context.awaitCompletion( 8, TimeUnit.MINUTES);

    }

    @AfterAll
    void deleteKafkaInstance(VertxTestContext context) {

        deleteKafkaByNameIfExists(api, KAFKA_INSTANCE_NAME)
                .onComplete(context.succeedingThenComplete());
    }

    void assertRestAPI() {
        assumeTrue(restAPI != null, "rest API is null because the bootstrap has failed");
    }
    void assertTopic() {
        assumeTrue(topic != null, "topic is null because the testCreateTopic has failed to create the topic on the Kafka instance");
    }
    void assertGroup() {
        assumeTrue(group != null, "group test don't work because canary group cannot be found");
    }

    @Test
    @Order(1)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testCreateTopic(VertxTestContext context)  {
        assertRestAPI();

        CreateTopicPayload topicPayload = setUpTopicPayload();

        restAPI.getTopicByNameNotExisting(TEST_TOPIC_NAME).compose(responseMessage -> {
            assertEquals("This server does not host this topic-partition.",responseMessage );
            return restAPI.createTopic(topicPayload);
            // Topic should not exist before inserting, calling API should result in 201 response code, and should be there after inserting
        })
                .onSuccess(createTopicResponse -> {
                    assertEquals(TEST_TOPIC_NAME, createTopicResponse.name);
                    topic = createTopicResponse.name;})
                .onComplete(context.succeedingThenComplete());

        context.completeNow();
    }

    @Test
    @Order(2)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testCreateExistingTopic(VertxTestContext context)  {
        assertRestAPI();
        assertTopic();
        CreateTopicPayload topicPayload = setUpTopicPayload();
        restAPI.createTopicAlreadyExisted(topicPayload)
                .onSuccess(responseMessage -> assertEquals("Topic '"+TEST_TOPIC_NAME+"' already exists.", responseMessage))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(3)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testGetTopicByName(VertxTestContext context)  {
        assertRestAPI();
        assertTopic();
        restAPI.getTopicByName(TEST_TOPIC_NAME)
                .onSuccess(topic -> assertEquals(TEST_TOPIC_NAME, topic.name))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(3)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testGetNotExistingTopicByName(VertxTestContext context) {
        assertRestAPI();
        restAPI.getTopicByNameNotExisting(TEST_NOT_EXISTING_TOPIC_NAME)
                .onSuccess(responseMessage -> assertEquals( "This server does not host this topic-partition.", responseMessage))
                .onComplete(context.succeedingThenComplete());
    }


    @Test
    @Order(3)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void tetGetTopics(VertxTestContext context)  {
        assertRestAPI();
        assertTopic();
        restAPI.getAllTopics()
                .onSuccess(topics -> context.verify( () ->
                {
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
        restAPI.deleteTopic(TEST_TOPIC_NAME)
                .onSuccess( responseMessage -> assertEquals("[ \""+ TEST_TOPIC_NAME +"\" ]",responseMessage   ))
                .onComplete(context.succeedingThenComplete());

    }

    // TODO check what is expected response according to never version of API
    @Test
    @Order(4)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testDeleteNotExistingTopicByName(VertxTestContext context) {
        restAPI.deleteTopicNotExisting(TEST_NOT_EXISTING_TOPIC_NAME)
                .onSuccess(responseMessage -> assertEquals("This server does not host this topic-partition." ,responseMessage ))
                .onComplete(context.succeedingThenComplete());

    }

    // in current API version all group responses are 200, so I double check for at least 1 topic (strimzi-canary-group)
    @Test
    @Order(2)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testGetGroups(VertxTestContext context) {
        assertGroup();
        restAPI.getAllGroups().onSuccess(groupResponse -> {
            int howManyGroups =  groupResponse.length;
            assertTrue(howManyGroups >= 1);
        }).onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(1)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testGetGroupByName(VertxTestContext context) {
        restAPI.getGroupByName(TEST_GROUP_NAME)
                .onSuccess(groupResponse -> context.verify( () ->
                {
                    assertEquals(groupResponse.id,TEST_GROUP_NAME );
                    assertEquals("STABLE", groupResponse.state);
                    group = groupResponse.id;
                }))
                .onComplete(context.succeedingThenComplete());
    }



    @Test
    @Order(1)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testGetNotExistingGroupByName(VertxTestContext context) {
        restAPI.getGroupByNameNotExisting(TEST_NOT_EXISTING_GROUP_NAME)
                .onSuccess(groupResponse -> context.verify( () -> assertEquals("DEAD", groupResponse.state )))
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testDeleteGroupByName(VertxTestContext context) {
        assertGroup();
        restAPI.deleteGroup(TEST_GROUP_NAME)
                .onSuccess(responseMessage -> context.verify( () -> assertEquals( "The group is not empty.", responseMessage))  )
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(1)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void testDeleteNotExistingGroupByName(VertxTestContext context) {
        restAPI.deleteGroupNotExist(TEST_NOT_EXISTING_GROUP_NAME)
                .onSuccess(responseMessage -> context.verify( () -> assertEquals( "The group id does not exist.", responseMessage)))
                .onComplete(context.succeedingThenComplete());
    }


    CreateTopicPayload setUpTopicPayload(){
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