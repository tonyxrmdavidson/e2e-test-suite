package io.managed.services.test;


import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Set;

import static io.managed.services.test.TestUtils.await;
import static io.managed.services.test.TestUtils.waitFor;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopic;
import static io.managed.services.test.client.kafka.KafkaUtils.applyTopics;
import static io.managed.services.test.client.serviceapi.MetricsUtils.collectTopicMetric;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.applyServiceAccount;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.getKafkaByName;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


@Tag(TestTag.SERVICE_API)
@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ServiceAPIUserMetricsTest extends TestBase {

    private static final Logger LOGGER = LogManager.getLogger(ServiceAPITest.class);

    private static final String IN_MESSAGES_METRIC = "kafka_server_brokertopicmetrics_messages_in_total";
    private static final int MESSAGE_COUNT = 17;

    // use the kafka long living instance
    private static final String KAFKA_INSTANCE_NAME = "mk-e2e-ll-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String SERVICE_ACCOUNT_NAME = "mk-e2e-metrics-sa-" + Environment.KAFKA_POSTFIX_NAME;

    // use a special topic
    private static final String TOPIC_NAME = "metric-test-topic";

    private ServiceAPI api;

    void assertAPI() {
        assumeTrue(api != null, "api is null because the bootstrap has failed");
    }

    @BeforeAll
    void bootstrap(Vertx vertx, VertxTestContext context) {
        ServiceAPIUtils.serviceAPI(vertx)
                .onSuccess(a -> api = a)
                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(1)
    void testMessageInTotalMetric(Vertx vertx) {
        assertAPI();

        // retrieve the kafka info
        var kafka = await(getKafkaByName(api, KAFKA_INSTANCE_NAME)).orElseThrow();

        // retrieve service account and reset the credentials
        var serviceAccount = await(applyServiceAccount(api, SERVICE_ACCOUNT_NAME));

        String bootstrapHost = kafka.bootstrapServerHost;
        String clientID = serviceAccount.clientID;
        String clientSecret = serviceAccount.clientSecret;

        var admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);

        // ensure the topic exists
        await(applyTopics(admin, Set.of(TOPIC_NAME)));

        // retrieve the current in messages before sending more
        var initialInMessages = await(api.queryMetrics(kafka.id).map(r -> collectTopicMetric(r.items, TOPIC_NAME, IN_MESSAGES_METRIC)));
        LOGGER.info("the topic '{}' started with '{}' in messages", TOPIC_NAME, initialInMessages);

        LOGGER.info("send {} message to the topic: {}", MESSAGE_COUNT, TOPIC_NAME);
        await(testTopic(vertx, bootstrapHost, clientID, clientSecret, TOPIC_NAME, MESSAGE_COUNT, 10, 100));

        var inMessages = await(waitFor(vertx, "metric to be updated", ofSeconds(1), ofSeconds(10), last -> api.queryMetrics(kafka.id)
                .map(r -> {
                    var in = collectTopicMetric(r.items, TOPIC_NAME, IN_MESSAGES_METRIC);
                    if (last) {
                        LOGGER.warn("last kafka_server_brokertopicmetrics_messages_in_total value: {}", in);
                    }
                    return Pair.with(initialInMessages + MESSAGE_COUNT == in, in);
                })));

        LOGGER.info("final in message count for topic '{}' is: {}", TOPIC_NAME, inMessages);
        assertEquals(inMessages, initialInMessages + MESSAGE_COUNT);
    }
}
