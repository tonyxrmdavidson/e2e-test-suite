package io.managed.services.test.client.registry;

import io.apicurio.rest.client.auth.Auth;
import io.managed.services.test.client.oauth.KeycloakUser;

import java.util.Map;

public class BearerAuth implements Auth {

    public static final String BEARER = "Bearer ";

    private final KeycloakUser user;

    public BearerAuth(KeycloakUser user) {
        this.user = user;
    }

    @Override
    public void apply(Map<String, String> requestHeaders) {
        // TODO: Fix me
//        if (user.expired()) {
//            user = sbwait(user.refresh());
//        }
        requestHeaders.put("Authorization", BEARER + user.getAccessToken());
    }
}
