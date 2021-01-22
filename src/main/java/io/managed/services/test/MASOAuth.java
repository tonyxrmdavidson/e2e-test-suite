package io.managed.services.test;

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

public class MASOAuth {
    private static final Logger LOGGER = LogManager.getLogger(MASOAuth.class);

    static final String KEYCLOAK_HOST = "https://keycloak-edge-redhat-rhoam-user-sso.apps.mas-sso-stage.1gzl.s1.devshift.org";
    static final String AUTH_PATH = "/auth/realms/mas-sso-staging/protocol/openid-connect/auth";
    static final String TOKEN_PATH = "/auth/realms/mas-sso-staging/protocol/openid-connect/token";
    static final String CLIENT_ID = "strimzi-ui";
    static final String REDIRECT_URI = "https://qaprodauth.cloud.redhat.com/beta/application-services/openshift-streams";

    final Vertx vertx;
    final OAuth2Auth oauth2;

    static public String getToken(User user) {
        return user.get("access_token");
    }

    public MASOAuth(Vertx vertx) {

        this.oauth2 = OAuth2Auth.create(vertx, new OAuth2Options()
                .setFlow(OAuth2FlowType.AUTH_CODE)
                .setClientID(CLIENT_ID)
                .setSite(KEYCLOAK_HOST)
                .setTokenPath(TOKEN_PATH)
                .setAuthorizationPath(AUTH_PATH)
        );
        this.vertx = vertx;
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
                .put("redirect_uri", REDIRECT_URI));

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
                .put("redirect_uri", REDIRECT_URI));
    }
}
