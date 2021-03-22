package io.managed.services.test.client.kafkaadminapi;

import io.managed.services.test.Environment;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.managed.services.test.Environment.*;

public class KafkaAdminAPIUtils  {
    private static final Logger LOGGER = LogManager.getLogger(io.managed.services.test.client.serviceapi.ServiceAPIUtils.class);

    public static Future<KafkaAdminAPI> restApi(Vertx vertx, String kafkaInstanceUrl) {
        return restAPI(vertx, Environment.SSO_USERNAME, Environment.SSO_PASSWORD, kafkaInstanceUrl);
    }

    public static Future<KafkaAdminAPI> restAPI(Vertx vertx, String username, String password, String kafkaInstanceUrl) {
        var auth = new KeycloakOAuth(vertx,
                MAS_SSO_REDHAT_KEYCLOAK_URI,
                MAS_SSO_REDHAT_REDIRECT_URI,
                MAS_SSO_REDHAT_REALM,
                MAS_SSO_REDHAT_CLIENT_ID);

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_USERNAME, Environment.SSO_PASSWORD);
        return auth.login(username, password)
                .map(user -> new KafkaAdminAPI(vertx, kafkaInstanceUrl, user));
    }

}


