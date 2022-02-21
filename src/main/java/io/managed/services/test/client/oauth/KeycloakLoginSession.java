package io.managed.services.test.client.oauth;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.oauth.OAuth20Service;
import io.managed.services.test.Environment;
import io.managed.services.test.RetryUtils;
import io.managed.services.test.ThrowingSupplier;
import io.managed.services.test.client.exception.ApacheResponseException;
import io.managed.services.test.client.exception.AttributeNotFoundException;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.cookie.BasicCookieStore;
import org.apache.hc.client5.http.entity.UrlEncodedFormEntity;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.internal.StringUtil;
import org.jsoup.nodes.FormElement;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.function.Function;

public class KeycloakLoginSession {
    private static final Logger LOGGER = LogManager.getLogger(KeycloakLoginSession.class);

    private final String username;
    private final String password;

    private final ClientSession session = new ClientSession();

    public KeycloakLoginSession(String username, String password) {
        this(Vertx.vertx(), username, password);
    }

    public KeycloakLoginSession(Vertx vertx, String username, String password) {
        this.username = username;
        this.password = password;
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

        try {
            var response = login(authURI, redirectURI, Environment.REDHAT_SSO_LOGIN_FORM_ID);
            response = followRedirects(response);
            assertResponse(response, HttpURLConnection.HTTP_OK);
        } catch (ApacheResponseException e) {
            return Future.failedFuture(e);
        }

        LOGGER.info("authentication completed; authURI={}", authURI);
        return Future.succeededFuture();
    }

    public Future<KeycloakUser> loginToRedHatSSO() {
        try {
            return Future.succeededFuture(login(
                Environment.REDHAT_SSO_URI,
                Environment.REDHAT_SSO_REDIRECT_URI,
                Environment.REDHAT_SSO_LOGIN_FORM_ID,
                Environment.REDHAT_SSO_REALM,
                Environment.REDHAT_SSO_CLIENT_ID));
        } catch (ApacheResponseException e) {
            return Future.failedFuture(e);
        }
    }

    public Future<KeycloakUser> loginToOpenshiftIdentity() {
        try {
            return Future.succeededFuture(login(
                Environment.OPENSHIFT_IDENTITY_URI,
                Environment.OPENSHIFT_IDENTITY_REDIRECT_URI,
                Environment.OPENSHIFT_IDENTITY_LOGIN_FORM_ID,
                Environment.OPENSHIFT_IDENTITY_REALM,
                Environment.OPENSHIFT_IDENTITY_CLIENT_ID));
        } catch (ApacheResponseException e) {
            return Future.failedFuture(e);
        }
    }

    /**
     * Login for the first time against the oauth realm using username and password and hook to the redirectURI
     * to retrieve the access code and complete the authentication to retrieve the access_token and refresh_token
     */
    public KeycloakUser login(String keycloakURI, String redirectURI, String loginFormId, String realm, String clientID)
        throws ApacheResponseException {

        var oauth2 = createOAuth2(keycloakURI, realm, clientID, redirectURI);
        var authURI = oauth2.getAuthorizationUrl();


        var response = login(authURI, redirectURI, loginFormId);

        var token = authenticateUser(oauth2, response);

        LOGGER.info("authentication completed");
        return new KeycloakUser(oauth2, token);
    }

    private CloseableHttpResponse login(String authURI, String redirectURI, String loginFormId)
        throws ApacheResponseException {

        var response = retry(() -> {
            try {
                return followAuthenticationRedirects(authURI, redirectURI, loginFormId);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertResponse(response, HttpURLConnection.HTTP_MOVED_TEMP);
        return response;
    }

    private CloseableHttpResponse followAuthenticationRedirects(String uri, String redirectURI, String loginFormId)
        throws ApacheResponseException, IOException {

        var request = new HttpGet(uri);
        var response = session.execute(request);
        return followAuthenticationRedirects(response, request, redirectURI, loginFormId);
    }

    private CloseableHttpResponse followAuthenticationRedirects(
        CloseableHttpResponse response,
        HttpUriRequestBase request,
        String redirectURI,
        String loginFormId) throws ApacheResponseException, IOException {

        if (response.getCode() == HttpURLConnection.HTTP_MOVED_TEMP) {

            var l = getRedirectLocation(response);
            if (l.contains(redirectURI)) {
                // the authentication is completed because we are redirected to the redirectURI
                return response;
            }
        }

        if (response.getCode() == HttpURLConnection.HTTP_OK) {

            String body;
            try {
                body = EntityUtils.toString(response.getEntity());
            } catch (ParseException e) {
                throw new ApacheResponseException(e, response);
            }

            var document = Jsoup.parse(body);

            var loginError = document.select("#rh-login-form-alert #rh-login-form-error-title").text();
            if (!loginError.isBlank()) {
                throw new ApacheResponseException(loginError, response, body);
            }

            var loginForm = document.select(loginFormId).forms().stream().findAny();
            if (loginForm.isPresent()) {

                // make a post request with the username and password and follow all redirects until the user
                // is authenticated
                try {
                    return postUsernamePassword(loginForm.get(), username, password, redirectURI, loginFormId);
                } catch (AttributeNotFoundException e) {
                    throw new ApacheResponseException(e, response, body);
                }
            }

            // we should be at the Grant Access page
            var form = document.getAllElements().forms().stream().findAny()
                .orElseThrow(() -> new ApacheResponseException("the response doesn't contain any <form>", response, body));

            try {
                return grantAccess(form, request.getRequestUri(), redirectURI, loginFormId);
            } catch (AttributeNotFoundException e) {
                throw new ApacheResponseException(e, response, body);
            }
        }

        if (response.getCode() >= 300 && response.getCode() < 400) {

            var l = getRedirectLocation(response);
            LOGGER.info("follow authentication redirect to: {}", l);

            return followAuthenticationRedirects(l, redirectURI, loginFormId);
        }

        throw new ApacheResponseException("Unexpected response", response);
    }

    private String getRedirectLocation(CloseableHttpResponse response) throws ApacheResponseException {
        var l = response.getFirstHeader("Location");
        if (l == null) {
            throw new ApacheResponseException("Location header not found", response);
        }
        return l.getValue();
    }

    private CloseableHttpResponse postUsernamePassword(
        FormElement form,
        String username,
        String password,
        String redirectURI,
        String loginFormId) throws AttributeNotFoundException, ApacheResponseException, IOException {

        // we are at the login page therefore we are going to post the username and password to proceed with the
        // authentication
        var actionURI = form.attr("action");
        if (actionURI.isEmpty()) {
            throw new AttributeNotFoundException("action URI not found");
        }
        var request = new HttpPost(actionURI);

        var pairs = new ArrayList<NameValuePair>();
        pairs.add(new BasicNameValuePair("username", username));
        pairs.add(new BasicNameValuePair("password", password));
        request.setEntity(new UrlEncodedFormEntity(pairs));

        LOGGER.info("post username and password; uri={}; username={}", actionURI, username);
        var response = session.execute(request);

        return followAuthenticationRedirects(response, request, redirectURI, loginFormId);
    }

    private CloseableHttpResponse grantAccess(
        FormElement form,
        String baseURI,
        String redirectURI,
        String loginFormId) throws AttributeNotFoundException, ApacheResponseException, IOException {

        var actionURI = form.attr("action");
        var code = form.select("input[name=code]").val();
        var accept = form.select("input[name=accept]").val();

        if (actionURI.isEmpty() || code.isEmpty() || accept.isEmpty()) {
            throw new AttributeNotFoundException("action URI, code input or accept input not found");
        }
        var request = new HttpPost(StringUtil.resolve(baseURI, actionURI));

        var pairs = new ArrayList<NameValuePair>();
        pairs.add(new BasicNameValuePair("code", code));
        pairs.add(new BasicNameValuePair("accept", accept));
        request.setEntity(new UrlEncodedFormEntity(pairs));

        LOGGER.info("grant access; uri: {}", actionURI);
        var response = session.execute(request);

        return followAuthenticationRedirects(response, request, redirectURI, loginFormId);
    }

    private OAuth2AccessToken authenticateUser(OAuth20Service oauth2, CloseableHttpResponse response)
        throws ApacheResponseException {

        var redirectLocation = getRedirectLocation(response);
        LOGGER.info("redirect location found: {}", redirectLocation);
        var auth = oauth2.extractAuthorization(redirectLocation);

        try {
            return oauth2.getAccessToken(auth.getCode());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CloseableHttpResponse followRedirects(CloseableHttpResponse response) throws ApacheResponseException {

        var c = response.getCode();
        if (c >= 300 && c < 400) {

            // handle redirects
            var l = getRedirectLocation(response);
            LOGGER.info("follow redirect to: {}", l);

            try (var nextResponse = session.execute(new HttpGet(l))) {
                return followRedirects(nextResponse);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return response;
    }

    public static void assertResponse(CloseableHttpResponse response, int statusCode) throws ApacheResponseException {
        if (response.getCode() != statusCode) {
            var message = String.format("Expected status code %d but got %s", statusCode, response.getCode());
            throw new ApacheResponseException(message, response);
        }
    }

    private <T, E extends Throwable> T retry(ThrowingSupplier<T, E> call) throws E {

        Function<Throwable, Boolean> condition = t -> {
            if (t instanceof ApacheResponseException) {
                var r = ((ApacheResponseException) t).response;
                // retry request in case of error 502
                return r.getCode() == HttpURLConnection.HTTP_BAD_GATEWAY;
            }
            return false;
        };

        return RetryUtils.retry(1, call, condition);
    }

    public String getUsername() {
        return username;
    }

    private static class ClientSession {

        private final CloseableHttpClient client;
        private final HttpClientContext context;

        private ClientSession() {

            context = HttpClientContext.create();
            context.setCookieStore(new BasicCookieStore());

            client = HttpClientBuilder.create()
                .disableRedirectHandling()
                .build();
        }

        private CloseableHttpResponse execute(final ClassicHttpRequest request) throws IOException {
            return client.execute(request, context);
        }
    }
}
