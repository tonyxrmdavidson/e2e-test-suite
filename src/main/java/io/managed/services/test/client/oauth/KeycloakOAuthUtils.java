package io.managed.services.test.client.oauth;

import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.oauth2.OAuth2Auth;
import io.vertx.ext.web.client.HttpResponse;
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
import java.util.function.Function;

import static io.managed.services.test.client.BaseVertxClient.getRedirectLocation;

public class KeycloakOAuthUtils {
    private static final Logger LOGGER = LogManager.getLogger(KeycloakOAuthUtils.class);

    public static Future<HttpResponse<Buffer>> followRedirects(
            WebClientSession session,
            HttpResponse<Buffer> response
    ) {
        return followRedirects(session, response, __ -> true);
    }

    public static Future<HttpResponse<Buffer>> followRedirects(
            WebClientSession session,
            HttpResponse<Buffer> response,
            Function<HttpResponse<Buffer>, Boolean> condition) {

        var c = response.statusCode();
        if ((c >= 300 && c < 400)
                && condition.apply(response)) {

            // handle redirects
            return getRedirectLocation(response)
                    .compose(l -> {
                        LOGGER.info("follow redirect to: {}", l);
                        return session.getAbs(l).send();
                    })
                    .compose(r -> followRedirects(session, r, condition));
        }

        return Future.succeededFuture(response);
    }

    public static Future<HttpResponse<Buffer>> startLogin(WebClientSession session, String authURI) {
        LOGGER.info("start oauth login; uri={}", authURI);
        return session.getAbs(authURI).send();
    }

    public static Future<HttpResponse<Buffer>> postUsernamePassword(
            WebClientSession session, HttpResponse<Buffer> response, String username, String password) {

        Document d = Jsoup.parse(response.bodyAsString());
        String actionURI = d.select("#kc-form-login").attr("action");

        MultiMap f = MultiMap.caseInsensitiveMultiMap();
        f.add("username", username);
        f.add("password", password);

        LOGGER.info("post username and password; uri={}; username={}", actionURI, username);
        return session.postAbs(actionURI).sendForm(f);
    }


    public static Future<User> authenticateUser(OAuth2Auth oauth2, String redirectURI, HttpResponse<Buffer> response) {

        return getRedirectLocation(response)
                .compose(locationURI -> {
                    List<NameValuePair> queries = URLEncodedUtils.parse(URI.create(locationURI), StandardCharsets.UTF_8);
                    String code = queries.stream()
                            .filter(v -> v.getName().equals("code")).findFirst()
                            .orElseThrow().getValue();

                    LOGGER.info("authenticate user; code={}", code);
                    return oauth2.authenticate(new JsonObject()
                            .put("code", code)
                            .put("redirect_uri", redirectURI));
                });

    }
}
