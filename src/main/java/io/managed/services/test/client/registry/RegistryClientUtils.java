package io.managed.services.test.client.registry;

import io.managed.services.test.client.oauth.KeycloakUser;

public class RegistryClientUtils {

    public static RegistryClientApi registryClient(String uri, KeycloakUser user) {
        return new RegistryClientApi(uri, user);
    }
}
