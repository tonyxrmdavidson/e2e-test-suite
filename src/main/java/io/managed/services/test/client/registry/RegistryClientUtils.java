package io.managed.services.test.client.registry;

import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.client.RegistryClientFactory;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;

public class RegistryClientUtils {
    private static final Logger LOGGER = LogManager.getLogger(RegistryClientUtils.class);

    public static Future<RegistryClient> registryClient(Vertx vertx, String registryUrl, String username, String password) {
        var auth = new KeycloakOAuth(vertx, username, password);

        LOGGER.info("authenticate user: {} against MAS SSO", username);
        return auth.loginToMASSSO()

            .map(user -> RegistryClientFactory.create(
                registryUrl,
                new HashMap<>(),
                new BearerAuth(KeycloakOAuth.getToken(user))));
    }
}
