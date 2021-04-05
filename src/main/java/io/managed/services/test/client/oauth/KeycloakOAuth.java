package io.managed.services.test.client.oauth;

import io.managed.services.test.client.BaseVertxClient;
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

import java.net.HttpURLConnection;

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

    private OAuth2Auth createOAuth2(
            String keycloakURI,
            String realm,
            String clientID) {

        var tokenPath = String.format(TOKEN_PATH_FORMAT, realm);
        var authPath = String.format(AUTH_PATH_FORMAT, realm);

        return OAuth2Auth.create(vertx, new OAuth2Options()
                .setFlow(OAuth2FlowType.AUTH_CODE)
                .setClientID(clientID)
                .setSite(keycloakURI)
                .setTokenPath(tokenPath)
                .setAuthorizationPath(authPath));
    }

    private String getAuthURI(OAuth2Auth oauth2, String redirectURI) {
        return oauth2.authorizeURL(new JsonObject()
                .put("redirect_uri", redirectURI));
    }

    /**
     * Login against the authURI for the first time and follow all  redirect back to the
     * redirectURI passed with the authURI.
     * <p>
     * Generally used for CLI authentication.
     */
    public Future<Void> login(String authURI, String username, String password) {
        return startLogin(session, authURI)
                .compose(r -> postUsernamePassword(session, r, username, password))
                .compose(r -> followRedirects(session, r))
                .compose(r -> BaseVertxClient.assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(__ -> {
                    LOGGER.info("authentication completed; authURI={}", authURI);
                    return null;
                });
    }

    /**
     * Login against the authURI using the stored session cookies to skip the username/password login
     */
    public Future<Void> login(String authURI) {
        return startLogin(session, authURI)
                .compose(r -> followRedirects(session, r))
                .compose(r -> BaseVertxClient.assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(__ -> {
                    LOGGER.info("authentication completed; authURI={}", authURI);
                    return null;
                });
    }

    /**
     * Login for the first time against the oauth realm using username and password and hook to the redirectURI
     * to retrieve the access code and complete the authentication to retrieve the access_token and refresh_token
     */
    public Future<User> login(
            String keycloakURI,
            String redirectURI,
            String realm,
            String clientID,
            String username,
            String password) {

        var oauth2 = createOAuth2(keycloakURI, realm, clientID);
        var authURI = getAuthURI(oauth2, redirectURI);

        return startLogin(session, authURI)
                .compose(r -> postUsernamePassword(session, r, username, password))
                .compose(r -> authenticateUser(session, oauth2, redirectURI, r))
                .map(u -> {
                    LOGGER.info("authentication completed; access_token={}", getToken(u));
                    return u;
                });
    }

    /**
     * Login against the oauth realm using the stored session cookies and hook to the redirectURI
     * to retrieve the access code and complete the authentication to retrieve the access_token and refresh_token
     */
    public Future<User> login(
            String keycloakURI,
            String redirectURI,
            String realm,
            String clientID) {

        var oauth2 = createOAuth2(keycloakURI, realm, clientID);
        var authURI = getAuthURI(oauth2, redirectURI);

        return startLogin(session, authURI)
                .compose(r -> authenticateUser(session, oauth2, redirectURI, r))
                .map(u -> {
                    LOGGER.info("authentication completed; access_token={}", getToken(u));
                    return u;
                });
    }

}
