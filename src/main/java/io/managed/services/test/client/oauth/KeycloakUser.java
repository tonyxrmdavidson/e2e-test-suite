package io.managed.services.test.client.oauth;

import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.oauth.OAuth20Service;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import javax.annotation.Nullable;
import java.util.Objects;

@Log4j2
public class KeycloakUser {

    @Nullable
    private final OAuth20Service service;

    private OAuth2AccessToken token;


    public KeycloakUser(String token) {
        this(null, new OAuth2AccessToken(token));
    }

    public KeycloakUser(OAuth2AccessToken token) {
        this(null, token);
    }

    public KeycloakUser(@Nullable OAuth20Service service, OAuth2AccessToken token) {
        this.service = service;
        this.token = Objects.requireNonNull(token);
    }

    @SneakyThrows
    synchronized public KeycloakUser renewToken() {
        if (service != null) {
            token = service.refreshAccessToken(token.getRefreshToken());
        } else {
            log.warn("skip token refresh");
        }
        return this;
    }

    public String getAccessToken() {
        return token.getAccessToken();
    }

    public String getRefreshToken() {
        return token.getRefreshToken();
    }
}
