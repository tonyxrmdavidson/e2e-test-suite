package io.managed.services.test.client.oauth;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.oauth2.OAuth2Auth;
import io.vertx.ext.auth.oauth2.OAuth2FlowType;
import io.vertx.ext.auth.oauth2.OAuth2Options;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.client.WebClientSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.managed.services.test.client.oauth.KeycloakOAuthUtils.authenticateUser;
import static io.managed.services.test.client.oauth.KeycloakOAuthUtils.followRedirects;
import static io.managed.services.test.client.oauth.KeycloakOAuthUtils.postUsernamePassword;
import static io.managed.services.test.client.oauth.KeycloakOAuthUtils.startLogin;

public class KeycloakOAuth {
    private static final Logger LOGGER = LogManager.getLogger(KeycloakOAuth.class);

    static final String AUTH_PATH_FORMAT = "/auth/realms/%s/protocol/openid-connect/auth";
    static final String TOKEN_PATH_FORMAT = "/auth/realms/%s/protocol/openid-connect/token";

    private final Vertx vertx;
    private final WebClientSession session;

    static public String getToken(User user) {
        return user.get("access_token");
    }

    static public String getRefreshToken(User user) {
        return user.get("refresh_token");
    }

    public KeycloakOAuth(Vertx vertx) {
        this.vertx = vertx;

        var client = WebClient.create(vertx, new WebClientOptions()
                .setFollowRedirects(false));

        this.session = WebClientSession.create(client);
    }

    public Future<User> login(
            String keycloakURI,
            String redirectURI,
            String realm,
            String clientID,
            String username,
            String password) {

        var tokenPath = String.format(TOKEN_PATH_FORMAT, realm);
        var authPath = String.format(AUTH_PATH_FORMAT, realm);

        var oauth2 = OAuth2Auth.create(vertx, new OAuth2Options()
                .setFlow(OAuth2FlowType.AUTH_CODE)
                .setClientID(clientID)
                .setSite(keycloakURI)
                .setTokenPath(tokenPath)
                .setAuthorizationPath(authPath));

        var authURI = oauth2.authorizeURL(new JsonObject()
                .put("redirect_uri", redirectURI));

        return startLogin(session, authURI)
                .compose(r -> followRedirects(session, r))
                .compose(r -> postUsernamePassword(session, r, username, password))
                .compose(r -> authenticateUser(session, oauth2, redirectURI, r))
                .map(u -> {
                    LOGGER.info("authentication completed; access_token={}", getToken(u));
                    return u;
                });
    }

    public Future<User> login(
            String keycloakURI,
            String redirectURI,
            String realm,
            String clientID) {

        var tokenPath = String.format(TOKEN_PATH_FORMAT, realm);
        var authPath = String.format(AUTH_PATH_FORMAT, realm);

        var oauth2 = OAuth2Auth.create(vertx, new OAuth2Options()
                .setFlow(OAuth2FlowType.AUTH_CODE)
                .setClientID(clientID)
                .setSite(keycloakURI)
                .setTokenPath(tokenPath)
                .setAuthorizationPath(authPath));

        var authURI = oauth2.authorizeURL(new JsonObject()
                .put("redirect_uri", redirectURI));

        return startLogin(session, authURI)
                .compose(r -> authenticateUser(session, oauth2, redirectURI, r))
                .map(u -> {
                    LOGGER.info("authentication completed; access_token={}", getToken(u));
                    return u;
                });
    }

}
