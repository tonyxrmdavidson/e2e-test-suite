package io.managed.services.test.client.restapi;

import io.managed.services.test.Environment;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.managed.services.test.Environment.*;

public class RestAPIUtils {
    private static final Logger LOGGER = LogManager.getLogger(io.managed.services.test.client.serviceapi.ServiceAPIUtils.class);

    public static Future<RestAPI> restApi(Vertx vertx, String kafkaInstanceUrl) {
        return RestAPI(vertx, Environment.SSO_USERNAME, Environment.SSO_PASSWORD, kafkaInstanceUrl);
    }

    public static Future<RestAPI> RestAPI(Vertx vertx, String username, String password, String kafkaInstanceUrl) {
        var auth = new KeycloakOAuth(vertx,
                MAS_SSO_REDHAT_KEYCLOAK_URI,
                MAS_SSO_REDHAT_REDIRECT_URI,
                MAS_SSO_REDHAT_REALM,
                MAS_SSO_REDHAT_CLIENT_ID);

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_USERNAME, Environment.SSO_PASSWORD);
        return auth.login(username, password)
                .map(user -> new RestAPI(vertx, kafkaInstanceUrl, user));
    }

}


