package io.managed.services.test.client.registry;

import io.apicurio.rest.client.auth.Auth;

import java.util.Map;

public class BearerAuth implements Auth {

    public static final String BEARER = "Bearer ";

    private String token;

    public void setAccessToken(String token) {
        this.token = token;
    }

    @Override
    public void apply(Map<String, String> requestHeaders) {
        requestHeaders.put("Authorization", BEARER + token);
    }
}
