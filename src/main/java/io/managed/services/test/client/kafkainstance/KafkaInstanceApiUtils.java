package io.managed.services.test.client.kafkainstance;

import com.openshift.cloud.api.kas.auth.invoker.ApiClient;
import com.openshift.cloud.api.kas.auth.models.AclBinding;
import com.openshift.cloud.api.kas.auth.models.AclOperation;
import com.openshift.cloud.api.kas.auth.models.AclPatternType;
import com.openshift.cloud.api.kas.auth.models.AclPermissionType;
import com.openshift.cloud.api.kas.auth.models.AclResourceType;
import com.openshift.cloud.api.kas.auth.models.ConsumerGroup;
import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.Topic;
import com.openshift.cloud.api.kas.auth.models.TopicSettings;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import io.managed.services.test.IsReady;
import io.managed.services.test.ThrowingFunction;
import io.managed.services.test.ThrowingSupplier;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiNotFoundException;
import io.managed.services.test.client.kafka.KafkaAuthMethod;
import io.managed.services.test.client.kafka.KafkaConsumerClient;
import io.managed.services.test.client.oauth.KeycloakLoginSession;
import io.managed.services.test.client.oauth.KeycloakUser;
import io.managed.services.test.wait.TReadyFunction;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.javatuples.Pair;

import java.util.Optional;
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
        return String.format("https://admin-server-%s/rest", bootstrapServerHost);
    }

    public static Future<KafkaInstanceApi> kafkaInstanceApi(KafkaRequest kafka, String username, String password) {
        return kafkaInstanceApi(new KeycloakLoginSession(username, password), kafka);
    }

    public static Future<KafkaInstanceApi> kafkaInstanceApi(KeycloakLoginSession auth, KafkaRequest kafka) {
        log.info("authenticate user '{}' against MAS SSO", auth.getUsername());
        return auth.loginToOpenshiftIdentity().map(u -> kafkaInstanceApi(kafkaInstanceApiUri(kafka), u));
    }

    public static KafkaInstanceApi kafkaInstanceApi(String uri, KeycloakUser user) {
        return new KafkaInstanceApi(new ApiClient().setBasePath(uri), user);
    }

    public static Future<KafkaConsumerClient<String, String>> startConsumerGroup(
        Vertx vertx,
        String consumerGroup,
        String topicName,
        String bootstrapServerHost,
        String clientId,
        String clientSecret) {

        log.info("start kafka consumer with group id '{}'", consumerGroup);
        var consumer = new KafkaConsumerClient<>(vertx,
            bootstrapServerHost,
            clientId,
            clientSecret,
            KafkaAuthMethod.OAUTH,
            consumerGroup,
            "latest",
            StringDeserializer.class,
            StringDeserializer.class);

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

    public static Optional<ConsumerGroup> getConsumerGroupByName(KafkaInstanceApi api, String consumerGroupId)
        throws ApiGenericException {

        try {
            return Optional.of(api.getConsumerGroupById(consumerGroupId));
        } catch (ApiNotFoundException e) {
            return Optional.empty();
        }
    }

    public static ConsumerGroup waitForConsumerGroup(KafkaInstanceApi api, String consumerGroupId)
        throws ApiGenericException, InterruptedException, TimeoutException {

        return waitForConsumerGroup(() -> getConsumerGroupByName(api, consumerGroupId));
    }

    public static <T extends Throwable> ConsumerGroup waitForConsumerGroup(
        ThrowingSupplier<Optional<ConsumerGroup>, T> supplier)
        throws T, InterruptedException, TimeoutException {

        var groupAtom = new AtomicReference<ConsumerGroup>();
        ThrowingFunction<Boolean, Boolean, T> ready = last -> {
            var exists = supplier.get();
            if (exists.isPresent()) {
                groupAtom.set(exists.get());
                return true;
            }
            return false;
        };

        // wait for the consumer group to show at least one consumer
        // because it could take a few seconds for the kafka admin api to
        // report the connected consumer
        waitFor("consumer group", ofSeconds(2), ofMinutes(1), ready);

        return groupAtom.get();
    }

    public static ConsumerGroup waitForConsumersInConsumerGroup(KafkaInstanceApi api, String consumerGroupId)
        throws ApiGenericException, InterruptedException, TimeoutException {

        TReadyFunction<ConsumerGroup, ApiGenericException> ready = (last, atom) -> {
            var group = api.getConsumerGroupById(consumerGroupId);
            atom.set(group);
            return group.getConsumers().size() > 0;
        };

        // wait for the consumer group to show at least one consumer
        // because it could take a few seconds for the kafka admin api to
        // report the connected consumer
        return waitFor("consumers in consumer group", ofSeconds(2), ofMinutes(1), ready);
    }

    public static Optional<Topic> getTopicByName(KafkaInstanceApi api, String name) throws ApiGenericException {
        try {
            return Optional.of(api.getTopic(name));
        } catch (ApiNotFoundException e) {
            return Optional.empty();
        }
    }

    public static Topic applyTopic(KafkaInstanceApi api, String topicName) throws ApiGenericException {
        var topicPayload = new NewTopicInput()
            .name(topicName)
            .settings(new TopicSettings().numPartitions(1));

        return applyTopic(api, topicPayload);
    }

    public static Topic applyTopic(KafkaInstanceApi api, NewTopicInput payload) throws ApiGenericException {
        var existing = getTopicByName(api, payload.getName());

        if (existing.isPresent()) {
            var topic = existing.get();
            log.warn("topic '{}' already exists", topic.getName());
            log.debug(topic);
            return topic;
        } else {
            log.info("create kafka instance '{}'", payload.getName());
            return api.createTopic(payload);
        }
    }

    /**
     * Convert service account clientID into ACL principal.
     *
     * @param clientID Service account clientID
     * @return The principal name for ACLs
     */
    public static String toPrincipal(String clientID) {
        return "User:" + clientID;
    }

    /**
     * Allow the principal to perform the operation on all resources of the given resource type
     *
     * @param api          KafkaInstanceApi
     * @param principal    The principal id like a service account client id
     * @param resourceType The resource type for which the operation will be allowed
     * @param operation    The operation that will be allowed
     */
    public static void createAllowAnyACL(KafkaInstanceApi api, String principal, AclResourceType resourceType, AclOperation operation)
        throws ApiGenericException {

        var acl = new AclBinding()
            .principal(principal)
            .resourceType(resourceType)
            .patternType(AclPatternType.LITERAL)
            .resourceName("*")
            .permission(AclPermissionType.ALLOW)
            .operation(operation);

        log.debug(acl);

        api.createAcl(acl);
    }

    public static void createReadAnyTopicACL(KafkaInstanceApi api, String principal) throws ApiGenericException {
        createAllowAnyACL(api, principal, AclResourceType.TOPIC, AclOperation.READ);
    }

    public static void createWriteAnyTopicACL(KafkaInstanceApi api, String principal) throws ApiGenericException {
        createAllowAnyACL(api, principal, AclResourceType.TOPIC, AclOperation.WRITE);
    }

    public static void createReadAnyGroupACL(KafkaInstanceApi api, String principal) throws ApiGenericException {
        createAllowAnyACL(api, principal, AclResourceType.GROUP, AclOperation.READ);
    }

    /**
     * Allow the principal to consume and produce messages from and to any topics and in any group.
     *
     * @param api       KafkaInstanceApi
     * @param principal The principal id like a service account client id
     */
    public static void createProducerAndConsumerACLs(KafkaInstanceApi api, String principal) throws ApiGenericException {
        createReadAnyGroupACL(api, principal);
        createReadAnyTopicACL(api, principal);
        createWriteAnyTopicACL(api, principal);
    }
}
