package io.managed.services.test.client.oauth;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.oauth.OAuth20Service;
import io.managed.services.test.Environment;
import io.managed.services.test.RetryUtils;
import io.managed.services.test.client.BaseVertxClient;
import io.managed.services.test.client.exception.ResponseException;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
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

import static io.managed.services.test.client.BaseVertxClient.getRedirectLocationAsFeature;
import static io.managed.services.test.client.oauth.KeycloakLoginUtils.authenticateUser;
import static io.managed.services.test.client.oauth.KeycloakLoginUtils.followRedirects;
import static io.managed.services.test.client.oauth.KeycloakLoginUtils.grantAccess;
import static io.managed.services.test.client.oauth.KeycloakLoginUtils.postUsernamePassword;
import static io.managed.services.test.client.oauth.KeycloakLoginUtils.startLogin;

public class KeycloakLoginSession {
    private static final Logger LOGGER = LogManager.getLogger(KeycloakLoginSession.class);

    static final String AUTH_PATH_FORMAT = "/auth/realms/%s/protocol/openid-connect/auth";
    static final String TOKEN_PATH_FORMAT = "/auth/realms/%s/protocol/openid-connect/token";

    private final String username;
    private final String password;

    private final Vertx vertx;
    private final VertxWebClientSession session;

    public KeycloakLoginSession(String username, String password) {
        this(Vertx.vertx(), username, password);
    }

    public KeycloakLoginSession(Vertx vertx, String username, String password) {
        this.vertx = vertx;
        this.username = username;
        this.password = password;

        var client = WebClient.create(vertx, new WebClientOptions()
            .setFollowRedirects(false));

        this.session = new VertxWebClientSession(WebClientSession.create(client));
    }

    private OAuth20Service createOAuth2(
        String baseUrl,
        String realm,
        String clientID,
        String callback) {

        return new ServiceBuilder(clientID)
            .callback(callback)
            .build(Keycloak2Api.instance(baseUrl, realm));
    }

    /**
     * Login against the authURI for the first time and follow all  redirect back to the
     * redirectURI passed with the authURI.
     * <p>
     * Generally used for CLI authentication.
     */
    public Future<Void> login(String authURI) {

        var redirectURI = "http://localhost";

        return login(authURI, redirectURI, Environment.REDHAT_SSO_LOGIN_FORM_ID)
            .compose(r -> followRedirects(session, r))
            .compose(r -> BaseVertxClient.assertResponse(r, HttpURLConnection.HTTP_OK))
            .map(__ -> {
                LOGGER.info("authentication completed; authURI={}", authURI);
                return null;
            });
    }

    public Future<KeycloakUser> loginToRedHatSSO() {
        return login(
            Environment.REDHAT_SSO_URI,
            Environment.REDHAT_SSO_REDIRECT_URI,
            Environment.REDHAT_SSO_LOGIN_FORM_ID,
            Environment.REDHAT_SSO_REALM,
            Environment.REDHAT_SSO_CLIENT_ID);
    }

    public Future<KeycloakUser> loginToOpenshiftIdentity() {
        return login(
            Environment.OPENSHIFT_IDENTITY_URI,
            Environment.OPENSHIFT_IDENTITY_REDIRECT_URI,
            Environment.OPENSHIFT_IDENTITY_LOGIN_FORM_ID,
            Environment.OPENSHIFT_IDENTITY_REALM,
            Environment.OPENSHIFT_IDENTITY_CLIENT_ID);
    }

    /**
     * Login for the first time against the oauth realm using username and password and hook to the redirectURI
     * to retrieve the access code and complete the authentication to retrieve the access_token and refresh_token
     */
    public Future<KeycloakUser> login(String keycloakURI, String redirectURI, String loginFormId, String realm, String clientID) {

        var oauth2 = createOAuth2(keycloakURI, realm, clientID, redirectURI);
        var authURI = oauth2.getAuthorizationUrl();

        return login(authURI, redirectURI, loginFormId)
            .map(r -> authenticateUser(oauth2, r))
            .map(t -> {
                LOGGER.info("authentication completed");
                return new KeycloakUser(oauth2, t);
            });
    }

    private Future<VertxHttpResponse> login(String authURI, String redirectURI, String loginFormId) {
        return retry(() -> startLogin(session, authURI)
            .compose(r -> followAuthenticationRedirects(session, r, redirectURI, loginFormId))
            .compose(r -> VertxWebClientSession.assertResponse(r, HttpURLConnection.HTTP_MOVED_TEMP)));
    }

    @SneakyThrows
    private Future<VertxHttpResponse> followAuthenticationRedirects(
        VertxWebClientSession session,
        VertxHttpResponse response,
        String redirectURI,
        String loginFormId) {

        if (response.statusCode() == HttpURLConnection.HTTP_MOVED_TEMP
            && response.getHeader("Location").contains(redirectURI)) {

            // the authentication is completed because we are redirected to the redirectURI
            return Future.succeededFuture(response);
        }

        if (response.statusCode() == HttpURLConnection.HTTP_OK) {

            var document = Jsoup.parse(response.bodyAsString());

            var loginError = document.select("#rh-login-form-error-title").text();
            if (!loginError.isBlank()) {
                return Future.failedFuture(new ResponseException(loginError, response));
            }

            var loginForm = document.select(loginFormId)
                .forms().stream().findAny();
            if (loginForm.isPresent()) {
                // we are at the login page therefore we are going to post the username and password to proceed with the
                // authentication
                return postUsernamePassword(session, loginForm.get(), username, password)
                    .recover(t -> Future.failedFuture(new ResponseException(t.getMessage(), response)))
                    .compose(r -> followAuthenticationRedirects(session, r, redirectURI, loginFormId));
            }

            // we should be at the Grant Access page
            var form = document.getAllElements().forms().stream().findAny()
                .orElseThrow(() -> new ResponseException("the response doesn't contain any <form>", response));
            return grantAccess(session, form, response.getRequest().getAbsoluteURI())
                .recover(t -> Future.failedFuture(new ResponseException(t.getMessage(), response)))
                .compose(r -> followAuthenticationRedirects(session, r, redirectURI, loginFormId));
        }

        if (response.statusCode() >= 300 && response.statusCode() < 400) {

            // handle redirects
            return getRedirectLocationAsFeature(response)
                .compose(l -> {
                    LOGGER.info("follow authentication redirect to: {}", l);
                    return session.getAbs(l).send();
                })
                .compose(r -> followAuthenticationRedirects(session, r, redirectURI, loginFormId));
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
