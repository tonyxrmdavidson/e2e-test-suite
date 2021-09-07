package io.managed.services.test.client.kafkainstance;

import com.openshift.cloud.api.kas.auth.models.ConsumerGroup;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import io.managed.services.test.Environment;
import io.managed.services.test.IsReady;
import io.managed.services.test.ThrowableFunction;
import io.managed.services.test.client.KasAuthApiClient;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiNotFoundException;
import io.managed.services.test.client.kafka.KafkaAuthMethod;
import io.managed.services.test.client.kafka.KafkaConsumerClient;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.User;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import lombok.extern.log4j.Log4j2;
import org.javatuples.Pair;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.managed.services.test.TestUtils.waitFor;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;

@Log4j2
public class KafkaInstanceApiUtils {

    public static String kafkaInstanceApiUri(KafkaRequest kafka) {
        return kafkaInstanceApiUri(kafka.getBootstrapServerHost());
    }

    public static String kafkaInstanceApiUri(String bootstrapServerHost) {
        return String.format("%s%s", Environment.KAFKA_ADMIN_API_SERVER_PREFIX, bootstrapServerHost);
    }

    public static Future<KafkaInstanceApi> kafkaInstanceApi(KafkaRequest kafka, String username, String password) {
        return kafkaInstanceApi(new KeycloakOAuth(username, password), kafka);
    }

    public static Future<KafkaInstanceApi> kafkaInstanceApi(KeycloakOAuth auth, KafkaRequest kafka) {
        log.info("authenticate user '{}' against MAS SSO", auth.getUsername());
        return auth.loginToMASSSO().map(u -> kafkaInstanceApi(kafkaInstanceApiUri(kafka), u));
    }

    public static KafkaInstanceApi kafkaInstanceApi(String uri, User user) {
        var token = KeycloakOAuth.getToken(user);
        return new KafkaInstanceApi(new KasAuthApiClient().basePath(uri).bearerToken(token).getApiClient());
    }

    public static Future<KafkaConsumer<String, String>> startConsumerGroup(
        Vertx vertx,
        String consumerGroup,
        String topicName,
        String bootstrapServerHost,
        String clientId,
        String clientSecret) {

        log.info("start kafka consumer with group id '{}'", consumerGroup);
        var consumer = KafkaConsumerClient.createConsumer(vertx,
            bootstrapServerHost,
            clientId,
            clientSecret,
            KafkaAuthMethod.OAUTH,
            consumerGroup,
            "latest");

        log.info("subscribe to topic '{}'", topicName);
        consumer.subscribe(topicName);
        consumer.handler(r -> {
            // ignore
        });

        IsReady<Void> subscribed = last -> consumer.assignment().map(partitions -> {
            var o = partitions.stream().filter(p -> p.getTopic().equals(topicName)).findAny();
            return Pair.with(o.isPresent(), null);
        });
        return waitFor(vertx, "consumer group to subscribe", ofSeconds(2), ofMinutes(2), subscribed)
            .map(__ -> consumer);
    }

    public static ConsumerGroup waitForConsumerGroup(KafkaInstanceApi api, String consumerGroupId)
        throws ApiGenericException, InterruptedException, TimeoutException {

        var groupAtom = new AtomicReference<ConsumerGroup>();

        ThrowableFunction<Boolean, Boolean, ApiGenericException> ready = last -> {
            try {
                var group = api.getConsumerGroupById(consumerGroupId, null, null, null, null);
                groupAtom.set(group);
                return true;
            } catch (ApiNotFoundException e) {
                return false;
            }
        };

        // wait for the consumer group to show at least one consumer
        // because it could take a few seconds for the kafka admin api to
        // report the connected consumer
        waitFor("consumers in consumer group", ofSeconds(2), ofMinutes(1), ready);

        return groupAtom.get();
    }
}
