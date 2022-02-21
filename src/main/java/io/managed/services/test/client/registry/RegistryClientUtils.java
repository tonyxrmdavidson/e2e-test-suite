package io.managed.services.test.client.registry;

import io.managed.services.test.client.exception.ApacheResponseException;
import io.managed.services.test.client.oauth.KeycloakLoginSession;
import io.managed.services.test.client.oauth.KeycloakUser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RegistryClientUtils {
    private static final Logger LOGGER = LogManager.getLogger(RegistryClientUtils.class);

    @Deprecated
    public static RegistryClientApi registryClient(String registryUrl, String username, String password) throws ApacheResponseException {
        var auth = new KeycloakLoginSession(username, password);
        return registryClient(registryUrl, auth);
    }

    @Deprecated
    public static RegistryClientApi registryClient(String registryUrl, KeycloakLoginSession auth) throws ApacheResponseException {
        LOGGER.info("authenticate user: {} against MAS SSO", auth.getUsername());
        var user = auth.loginToOpenshiftIdentity();
        return registryClient(registryUrl, user);
    }

    public static RegistryClientApi registryClient(String uri, KeycloakUser user) {
        return new RegistryClientApi(uri, user);
    }
}
