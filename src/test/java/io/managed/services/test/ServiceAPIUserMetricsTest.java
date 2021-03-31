package io.managed.services.test;


import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.kafkaadminapi.KafkaAdminAPIUtils;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Timeout;
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
import org.opentest4j.TestAbortedException;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.managed.services.test.TestUtils.message;
import static io.managed.services.test.TestUtils.waitFor;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopicWithOauth;


import static io.managed.services.test.client.kafkaadminapi.KafkaAdminAPIUtils.applyTopics;
import static io.managed.services.test.client.serviceapi.MetricsUtils.collectTopicMetric;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.applyServiceAccount;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.getKafkaByName;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


@Tag(TestTag.SERVICE_API)
@ExtendWith(VertxExtension.class)
@Timeout(value = 5, timeUnit = TimeUnit.MINUTES)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ServiceAPIUserMetricsTest extends TestBase {

    private static final Logger LOGGER = LogManager.getLogger(ServiceAPITest.class);

    private static final String IN_MESSAGES_METRIC = "kafka_server_brokertopicmetrics_messages_in_total";
    private static final int MESSAGE_COUNT = 17;
    private static final Duration WAIT_FOR_METRIC_TIMEOUT = Duration.ofMinutes(2);

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
                .compose(a -> {
                    api = a;
                    return getKafkaByName(api, KAFKA_INSTANCE_NAME);
                })
                .compose(o -> o.map(Future::succeededFuture).orElseThrow(() -> new TestAbortedException(message("can't find the long living kafka instance: {}", KAFKA_INSTANCE_NAME))))
                .onComplete(context.succeedingThenComplete());

    }

    @Test
    @Order(1)
    void testMessageInTotalMetric(Vertx vertx, VertxTestContext context) {
        assertAPI();

        LOGGER.info("start testing message in total metric");

        // retrieve the kafka info
        var kafkaF = getKafkaByName(api, KAFKA_INSTANCE_NAME)
                .map(o -> o.orElseThrow(() -> new TestAbortedException(message("can't find the long living kafka instance: {}", KAFKA_INSTANCE_NAME))));


        var serviceAccountF = applyServiceAccount(api, SERVICE_ACCOUNT_NAME);

        var adminF = CompositeFuture.all(kafkaF, serviceAccountF)
                .map(__ -> {
                    String bootstrapHost = kafkaF.result().bootstrapServerHost;
                    String clientID = serviceAccountF.result().clientID;
                    String clientSecret = serviceAccountF.result().clientSecret;

                    return new KafkaAdmin(bootstrapHost, clientID, clientSecret);
                });

        var kafkaAdminApiF = kafkaF
                .compose(kafkaResponse -> KafkaAdminAPIUtils.restApiDefault(vertx, kafkaResponse.bootstrapServerHost));

        // ensure the topic exists
        var topicF = adminF
                .compose(__ -> {
                    LOGGER.info("ensure the topic {} exists", TOPIC_NAME);
                    return applyTopics(kafkaAdminApiF.result(), Set.of(TOPIC_NAME));
                });

        // retrieve the current in messages before sending more
        var initialInMessagesF = topicF
                .compose(__ -> api.queryMetrics(kafkaF.result().id))
                .map(r -> collectTopicMetric(r.items, TOPIC_NAME, IN_MESSAGES_METRIC))
                .onSuccess(i -> LOGGER.info("the topic '{}' started with '{}' in messages", TOPIC_NAME, i));

        // send n messages to the topic
        var testTopicF = initialInMessagesF
                .compose(__ -> {
                    String bootstrapHost = kafkaF.result().bootstrapServerHost;
                    String clientID = serviceAccountF.result().clientID;
                    String clientSecret = serviceAccountF.result().clientSecret;

                    LOGGER.info("send {} message to the topic: {}", MESSAGE_COUNT, TOPIC_NAME);
                    return testTopicWithOauth(vertx, bootstrapHost, clientID, clientSecret, TOPIC_NAME, MESSAGE_COUNT, 10, 100);
                });

        // wait for the metric to be updated or fail with timeout
        IsReady<Double> isMetricUpdated = last -> api.queryMetrics(kafkaF.result().id)
                .map(r -> {
                    var in = collectTopicMetric(r.items, TOPIC_NAME, IN_MESSAGES_METRIC);
                    if (last) {
                        LOGGER.warn("last kafka_server_brokertopicmetrics_messages_in_total value: {}", in);
                    }

                    var initialInMessage = initialInMessagesF.result();
                    return Pair.with(initialInMessage + MESSAGE_COUNT == in, in);
                });
        var inMessagesF = testTopicF
                .compose(__ -> waitFor(vertx, "metric to be updated", ofSeconds(3), WAIT_FOR_METRIC_TIMEOUT, isMetricUpdated));

        inMessagesF
                .onSuccess(i -> LOGGER.info("final in message count for topic '{}' is: {}", TOPIC_NAME, i))
                .onSuccess(i -> context.verify(() -> assertEquals(i, initialInMessagesF.result() + MESSAGE_COUNT)))
                .onComplete(context.succeedingThenComplete());
    }
}
