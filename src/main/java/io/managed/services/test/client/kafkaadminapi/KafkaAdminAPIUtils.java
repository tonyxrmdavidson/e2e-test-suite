package io.managed.services.test.client.kafkaadminapi;

import io.managed.services.test.Environment;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.managed.services.test.TestUtils.forEach;


public class KafkaAdminAPIUtils {
    private static final Logger LOGGER = LogManager.getLogger(KafkaAdminAPIUtils.class);

    public static Future<KafkaAdminAPI> kafkaAdminAPI(Vertx vertx, String bootstrapHost) {
        return kafkaAdminAPI(vertx, bootstrapHost, Environment.SSO_USERNAME, Environment.SSO_PASSWORD);

    }
    public static Future<KafkaAdminAPI> kafkaAdminAPI(Vertx vertx, String bootstrapHost, String username, String password) {
        var apiURI = String.format("%s%s", Environment.KAFKA_ADMIN_API_SERVER_PREFIX, bootstrapHost);
        var auth = new KeycloakOAuth(vertx);

        LOGGER.info("authenticate user: {} against: {}", username, Environment.MAS_SSO_REDHAT_KEYCLOAK_URI);
        return auth.login(
                Environment.MAS_SSO_REDHAT_KEYCLOAK_URI,
                Environment.MAS_SSO_REDHAT_REDIRECT_URI,
                Environment.MAS_SSO_REDHAT_REALM,
                Environment.MAS_SSO_REDHAT_CLIENT_ID,
                username, password)

                .map(user -> new KafkaAdminAPI(vertx, apiURI, user));
    }

    /**
     * Create a topic with the passed name using the default payload
     *
     * @param api       KafkaAdminAPI
     * @param topicName String
     * @return Void
     */
    static public Future<Topic> createDefaultTopic(KafkaAdminAPI api, String topicName) {
        var topicPayload = setUpDefaultTopicPayload(topicName);
        return api.createTopic(topicPayload);
    }

    /**
     * Create the missing topics
     *
     * @return the list of missing topics that has been created
     */
    static public Future<List<String>> applyTopics(KafkaAdminAPI admin, Set<String> topics) {

        List<String> missingTopics = new ArrayList<>();

        return admin.getAllTopics()

                // create the missing topics
                .compose(currentTopics -> forEach(topics.iterator(), t -> {
                    if (currentTopics.items.stream().anyMatch(o -> o.name.equals(t))) {
                        return Future.succeededFuture();
                    }

                    missingTopics.add(t);

                    LOGGER.info("create missing topic: {}", t);
                    return createDefaultTopic(admin, t).map(__ -> null);

                }).map(v -> missingTopics));
    }

    static public CreateTopicPayload setUpDefaultTopicPayload(String name) {
        var topicPayload = new CreateTopicPayload();
        topicPayload.name = name;
        topicPayload.settings = new CreateTopicPayload.Settings();

        // Partitions needs to be set to 1 in order sending messages properly in test suite ServiceApiLongLiveTest
        topicPayload.settings.numPartitions = 1;
        var c1 = new TopicConfig();
        var c2 = new TopicConfig();
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
