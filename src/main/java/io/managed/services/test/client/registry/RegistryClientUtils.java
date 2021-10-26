package io.managed.services.test.client.registry;

import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.client.RegistryClientFactory;
import io.managed.services.test.client.oauth.KeycloakLoginSession;
import io.managed.services.test.client.oauth.KeycloakUser;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;

public class RegistryClientUtils {
    private static final Logger LOGGER = LogManager.getLogger(RegistryClientUtils.class);

    public static Future<RegistryClient> registryClient(Vertx vertx, String registryUrl, String username, String password) {
        var auth = new KeycloakLoginSession(vertx, username, password);
        return registryClient(registryUrl, auth);
    }

    public static Future<RegistryClient> registryClient(String registryUrl, KeycloakLoginSession auth) {
        LOGGER.info("authenticate user: {} against MAS SSO", auth.getUsername());
        return auth.loginToOpenshiftIdentity()
            .map(user -> registryClient(registryUrl, user));
    }


    public static RegistryClient registryClient(String uri, KeycloakUser user) {
        return RegistryClientFactory.create(uri, new HashMap<>(), new BearerAuth(user));
    }
}
