package io.managed.services.test.client.oauth;

import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.oauth2.OAuth2Auth;
import io.vertx.ext.auth.oauth2.OAuth2FlowType;
import io.vertx.ext.auth.oauth2.OAuth2Options;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientSession;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class KeycloakOAuth {
    private static final Logger LOGGER = LogManager.getLogger(KeycloakOAuth.class);

    static final String AUTH_PATH_FORMAT = "/auth/realms/%s/protocol/openid-connect/auth";
    static final String TOKEN_PATH_FORMAT = "/auth/realms/%s/protocol/openid-connect/token";

    final Vertx vertx;
    final OAuth2Auth oauth2;
    final String redirectURI;

    static public String getToken(User user) {
        return user.get("access_token");
    }

    public KeycloakOAuth(Vertx vertx, String keycloakURI, String redirectURI, String realm, String clientID) {

        String tokenPath = String.format(TOKEN_PATH_FORMAT, realm);
        String authPath = String.format(AUTH_PATH_FORMAT, realm);

        this.oauth2 = OAuth2Auth.create(vertx, new OAuth2Options()
                .setFlow(OAuth2FlowType.AUTH_CODE)
                .setClientID(clientID)
                .setSite(keycloakURI)
                .setTokenPath(tokenPath)
                .setAuthorizationPath(authPath));
        this.vertx = vertx;
        this.redirectURI = redirectURI;
    }

    public Future<User> login(String username, String password) {
        if (username == null) {
            throw new IllegalArgumentException("username is null");
        }
        if (password == null) {
            throw new IllegalArgumentException("password is null");
        }

        Promise<User> p = Promise.promise();


        WebClient client = WebClient.create(vertx);
        WebClientSession session = WebClientSession.create(client);

        String authURI = oauth2.authorizeURL(new JsonObject()
                .put("redirect_uri", redirectURI));

        return startLogin(session, authURI)
                .flatMap(r -> postUsernamePassword(session, r, username, password))
                .flatMap(r -> authenticateUser(session, r))
                .map(u -> {
                    LOGGER.info("authentication completed; access_token={}", getToken(u));
                    return u;
                });
    }

    Future<HttpResponse<Buffer>> startLogin(WebClientSession session, String authURI) {
        LOGGER.info("start oauth login; uri={}", authURI);
        Promise<HttpResponse<Buffer>> p = Promise.promise();
        session.getAbs(authURI).send(p);
        return p.future();
    }

    Future<HttpResponse<Buffer>> postUsernamePassword(
            WebClientSession session, HttpResponse<Buffer> response, String username, String password) {

        Document d = Jsoup.parse(response.bodyAsString());
        String actionURI = d.select("#kc-form-login").attr("action");

        MultiMap f = MultiMap.caseInsensitiveMultiMap();
        f.add("username", username);
        f.add("password", password);

        LOGGER.info("post username and password; uri={}; username={}", actionURI, username);
        Promise<HttpResponse<Buffer>> p = Promise.promise();
        session.postAbs(actionURI).sendForm(f, p);
        return p.future();
    }

    Future<User> authenticateUser(WebClientSession session, HttpResponse<Buffer> response) {
        String locationURI = response.headers().get("location");
        List<NameValuePair> queries = URLEncodedUtils.parse(URI.create(locationURI), StandardCharsets.UTF_8);
        String code = queries.stream()
                .filter(v -> v.getName().equals("code")).findFirst()
                .orElseThrow().getValue();

        LOGGER.info("authenticate user; code={}", code);
        return oauth2.authenticate(new JsonObject()
                .put("code", code)
                .put("redirect_uri", redirectURI));
    }
}
