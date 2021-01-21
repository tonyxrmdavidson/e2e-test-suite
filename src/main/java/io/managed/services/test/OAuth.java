package io.managed.services.test;

import io.netty.handler.codec.http.QueryStringEncoder;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
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

public class OAuth {
    private static final Logger LOGGER = LogManager.getLogger(OAuth.class);

    final Boolean KEYCLOAK_TLS = true;
    final String KEYCLOAK_HOST = "keycloak-edge-redhat-rhoam-user-sso.apps.mas-sso-stage.1gzl.s1.devshift.org";
    final Integer KEYCLOAK_PORT = 443;
    final String AUTH_PATH = "/auth/realms/mas-sso-staging/protocol/openid-connect/auth";
    final String TOKEN_PATH = "/auth/realms/mas-sso-staging/protocol/openid-connect/token";
    final String CLIENT_ID = "strimzi-ui";
    final String REDIRECT_URI = "https://qaprodauth.cloud.redhat.com/beta/application-services/openshift-streams";
    final String RESPONSE_MODE = "query";
    final String RESPONSE_TYPE = "code";
    final String SCOPE = "openid";

    final WebClient client;
    final WebClientSession session;
    final String username;
    final String password;

    public OAuth(Vertx vertx, String username, String password) {
        if (username == null) {
            throw new IllegalArgumentException("username is null");
        }
        if (password == null) {
            throw new IllegalArgumentException("password is null");
        }

        WebClientOptions options = new WebClientOptions();
        options.setSsl(KEYCLOAK_TLS);
        options.setDefaultHost(KEYCLOAK_HOST);
        options.setDefaultPort(KEYCLOAK_PORT);

        this.client = WebClient.create(vertx, options);
        this.session = WebClientSession.create(this.client);
        this.username = username;
        this.password = password;
    }

    public Future<HttpResponse<Buffer>> auth() {
        return startLogin()
                .flatMap(this::postUsernamePassword)
                .flatMap(this::requestAccessToken);
    }

    Future<HttpResponse<Buffer>> startLogin() {

        QueryStringEncoder e = new QueryStringEncoder(AUTH_PATH);
        e.addParam("client_id", CLIENT_ID);
        e.addParam("redirect_uri", REDIRECT_URI);
        e.addParam("response_mode", RESPONSE_MODE);
        e.addParam("response_type", RESPONSE_TYPE);
        e.addParam("scope", SCOPE);
        String u = e.toString();

        LOGGER.info("start oauth login; host={}; uri={}", KEYCLOAK_HOST, u);
        Promise<HttpResponse<Buffer>> p = Promise.promise();
        session.get(u).send(p);
        return p.future();
    }

    Future<HttpResponse<Buffer>> postUsernamePassword(HttpResponse<Buffer> response) {

        Document d = Jsoup.parse(response.bodyAsString());
        String u = d.select("#kc-form-login").attr("action");

        URI uri = URI.create(u);
        u = uri.getPath() + '?' + uri.getQuery();

        MultiMap f = MultiMap.caseInsensitiveMultiMap();
        f.add("username", username);
        f.add("password", password);
        f.add("credentialId", "");

        LOGGER.info("post username and password; host={}; uri={}; username={}", KEYCLOAK_HOST, u, username);
        Promise<HttpResponse<Buffer>> p = Promise.promise();
        session.post(u).sendForm(f, p);
        return p.future();
    }

    Future<HttpResponse<Buffer>> requestAccessToken(HttpResponse<Buffer> response) {

        String u = response.headers().get("location");
        List<NameValuePair> queries = URLEncodedUtils.parse(URI.create(u), StandardCharsets.UTF_8);
        String code = queries.stream().filter(v -> v.getName().equals("code")).findFirst().orElseThrow().getValue();

        MultiMap f = MultiMap.caseInsensitiveMultiMap();
        f.add("grant_type", "authorization_code");
        f.add("client_id", CLIENT_ID);
        f.add("code", code);
        f.add("redirect_uri", REDIRECT_URI);

        LOGGER.info("request access token; host={}; uri={}; code={}", KEYCLOAK_HOST, TOKEN_PATH, code);
        Promise<HttpResponse<Buffer>> p = Promise.promise();
        session.post(TOKEN_PATH).sendForm(f, p);
        return p.future();
    }

}
