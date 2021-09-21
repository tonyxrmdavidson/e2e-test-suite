package io.managed.services.test.client.oauth;

import io.managed.services.test.Environment;
import io.managed.services.test.RetryUtils;
import io.managed.services.test.client.BaseVertxClient;
import io.managed.services.test.client.exception.ResponseException;
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
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;

import java.net.HttpURLConnection;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.managed.services.test.client.BaseVertxClient.getRedirectLocation;
import static io.managed.services.test.client.oauth.KeycloakOAuthUtils.authenticateUser;
import static io.managed.services.test.client.oauth.KeycloakOAuthUtils.followRedirects;
import static io.managed.services.test.client.oauth.KeycloakOAuthUtils.grantAccess;
import static io.managed.services.test.client.oauth.KeycloakOAuthUtils.postUsernamePassword;
import static io.managed.services.test.client.oauth.KeycloakOAuthUtils.startLogin;

public class KeycloakOAuth {
    private static final Logger LOGGER = LogManager.getLogger(KeycloakOAuth.class);

    static final String AUTH_PATH_FORMAT = "/auth/realms/%s/protocol/openid-connect/auth";
    static final String TOKEN_PATH_FORMAT = "/auth/realms/%s/protocol/openid-connect/token";

    private final String username;
    private final String password;

    private final Vertx vertx;
    private final VertxWebClientSession session;

    static public String getToken(User user) {
        return user.get("access_token");
    }

    static public String getRefreshToken(User user) {
        return user.get("refresh_token");
    }

    public KeycloakOAuth(String username, String password) {
        this(Vertx.vertx(), username, password);
    }

    public KeycloakOAuth(Vertx vertx, String username, String password) {
        this.vertx = vertx;
        this.username = username;
        this.password = password;

        var client = WebClient.create(vertx, new WebClientOptions()
            .setFollowRedirects(false));

        this.session = new VertxWebClientSession(WebClientSession.create(client));
    }

    private OAuth2Auth createOAuth2(
        String keycloakURI,
        String realm,
        String clientID) {

        var tokenPath = String.format(TOKEN_PATH_FORMAT, realm);
        var authPath = String.format(AUTH_PATH_FORMAT, realm);

        return OAuth2Auth.create(vertx, new OAuth2Options()
            .setFlow(OAuth2FlowType.AUTH_CODE)
            .setClientId(clientID)
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
    public Future<Void> login(String authURI) {

        var redirectURI = "http://localhost";

        return login(authURI, redirectURI)
            .compose(r -> followRedirects(session, r))
            .compose(r -> BaseVertxClient.assertResponse(r, HttpURLConnection.HTTP_OK))
            .map(__ -> {
                LOGGER.info("authentication completed; authURI={}", authURI);
                return null;
            });
    }

    public Future<User> loginToRHSSO() {
        return login(
            Environment.SSO_REDHAT_KEYCLOAK_URI,
            Environment.SSO_REDHAT_REDIRECT_URI,
            Environment.SSO_REDHAT_REALM,
            Environment.SSO_REDHAT_CLIENT_ID);
    }

    public Future<User> loginToMASSSO() {
        return login(
            Environment.MAS_SSO_REDHAT_KEYCLOAK_URI,
            Environment.MAS_SSO_REDHAT_REDIRECT_URI,
            Environment.MAS_SSO_REDHAT_REALM,
            Environment.MAS_SSO_REDHAT_CLIENT_ID);
    }

    /**
     * Login for the first time against the oauth realm using username and password and hook to the redirectURI
     * to retrieve the access code and complete the authentication to retrieve the access_token and refresh_token
     */
    public Future<User> login(String keycloakURI, String redirectURI, String realm, String clientID) {

        var oauth2 = createOAuth2(keycloakURI, realm, clientID);
        var authURI = getAuthURI(oauth2, redirectURI);

        return login(authURI, redirectURI)
            .compose(r -> authenticateUser(vertx, oauth2, redirectURI, r))
            .map(u -> {
                LOGGER.info("authentication completed");
                return u;
            });
    }

    private Future<VertxHttpResponse> login(String authURI, String redirectURI) {
        return retry(() -> startLogin(session, authURI)
            .compose(r -> followAuthenticationRedirects(session, r, redirectURI))
            .compose(r -> VertxWebClientSession.assertResponse(r, HttpURLConnection.HTTP_MOVED_TEMP)));
    }

    @SneakyThrows
    private Future<VertxHttpResponse> followAuthenticationRedirects(
        VertxWebClientSession session,
        VertxHttpResponse response,
        String redirectURI) {

        if (response.statusCode() == HttpURLConnection.HTTP_MOVED_TEMP
            && response.getHeader("Location").contains(redirectURI)) {

            // the authentication is completed because we are redirected to the redirectURI
            return Future.succeededFuture(response);
        }

        if (response.statusCode() == HttpURLConnection.HTTP_OK) {

            var document = Jsoup.parse(response.bodyAsString());

            var loginForm = document.select("#rh-password-verification-form")
                .forms().stream().findAny();
            if (loginForm.isPresent()) {
                // we are at the login page therefore we are going to post the username and password to proceed with the
                // authentication
                return postUsernamePassword(session, loginForm.get(), username, password)
                    .recover(t -> Future.failedFuture(new ResponseException(t.getMessage(), response)))
                    .compose(r -> followAuthenticationRedirects(session, r, redirectURI));
            }

            // we should be at the Grant Access page
            var form = document.getAllElements().forms().stream().findAny()
                .orElseThrow(() -> new ResponseException("the response doesn't contain any <form>", response));
            return grantAccess(session, form, response.getRequest().getAbsoluteURI())
                .recover(t -> Future.failedFuture(new ResponseException(t.getMessage(), response)))
                .compose(r -> followAuthenticationRedirects(session, r, redirectURI));
        }

        if (response.statusCode() >= 300 && response.statusCode() < 400) {

            // handle redirects
            return getRedirectLocation(response)
                .compose(l -> {
                    LOGGER.info("follow authentication redirect to: {}", l);
                    return session.getAbs(l).send();
                })
                .compose(r -> followAuthenticationRedirects(session, r, redirectURI));
        }

        return Future.succeededFuture(response);
    }

    private <T> Future<T> retry(Supplier<Future<T>> call) {

        Function<Throwable, Boolean> condition = t -> {
            if (t instanceof ResponseException) {
                var r = ((ResponseException) t).response;
                // retry request in case of error 502
                return r.statusCode() == HttpURLConnection.HTTP_BAD_GATEWAY;
            }

            return false;
        };

        return RetryUtils.retry(vertx, 1, call, condition);
    }

    public String getUsername() {
        return username;
    }
}
