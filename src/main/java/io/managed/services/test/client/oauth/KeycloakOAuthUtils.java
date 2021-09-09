package io.managed.services.test.client.oauth;

import io.managed.services.test.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.oauth2.OAuth2Auth;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.internal.StringUtil;
import org.jsoup.nodes.FormElement;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.managed.services.test.client.BaseVertxClient.getRedirectLocation;

public class KeycloakOAuthUtils {
    private static final Logger LOGGER = LogManager.getLogger(KeycloakOAuthUtils.class);

    public static Future<VertxHttpResponse> followRedirects(
        VertxWebClientSession session, VertxHttpResponse response) {

        var c = response.statusCode();
        if (c >= 300 && c < 400) {

            // handle redirects
            return getRedirectLocation(response)
                .compose(l -> {
                    LOGGER.info("follow redirect to: {}", l);
                    return session.getAbs(l).send();
                })
                .compose(r -> followRedirects(session, r));
        }

        return Future.succeededFuture(response);
    }

    public static Future<VertxHttpResponse> startLogin(VertxWebClientSession session, String authURI) {
        LOGGER.info("start oauth login; uri={}", authURI);
        return session.getAbs(authURI).send();
    }

    public static Future<VertxHttpResponse> postUsernamePassword(
        VertxWebClientSession session, FormElement form, String username, String password) {

        String actionURI = form.attr("action");

        var f = MultiMap.caseInsensitiveMultiMap();
        f.add("username", username);
        f.add("password", password);

        if (actionURI.isEmpty()) {
            return Future.failedFuture("action URI not found");
        }

        LOGGER.info("post username and password; uri={}; username={}", actionURI, username);
        return session.postAbs(actionURI).sendForm(f);
    }

    public static Future<VertxHttpResponse> grantAccess(
        VertxWebClientSession session, FormElement form, String baseURI) {

        var actionURI = form.attr("action");

        var code = form.select("input[name=code]").val();
        var accept = form.select("input[name=accept]").val();

        if (actionURI.isEmpty() || code.isEmpty() || accept.isEmpty()) {
            return Future.failedFuture("action URI, code input or accept input not found");
        }

        actionURI = StringUtil.resolve(baseURI, actionURI);

        var f = MultiMap.caseInsensitiveMultiMap();
        f.add("code", code);
        f.add("accept", accept);

        LOGGER.info("grant access; uri: {}", actionURI);
        return session.postAbs(actionURI).sendForm(f);
    }

    public static Future<User> authenticateUser(Vertx vertx, OAuth2Auth oauth2, String redirectURI, HttpResponse<Buffer> response) {

        return getRedirectLocation(response)
            .compose(locationURI -> {
                List<NameValuePair> queries = URLEncodedUtils.parse(URI.create(locationURI), StandardCharsets.UTF_8);
                String code = queries.stream()
                    .filter(v -> v.getName().equals("code")).findFirst()
                    .orElseThrow().getValue();

                LOGGER.info("authenticate user; code={}", code);
                return retry(vertx, () -> oauth2.authenticate(new JsonObject()
                    .put("code", code)
                    .put("redirectUri", redirectURI)));
            });

    }

    private static <T> Future<T> retry(Vertx vertx, Supplier<Future<T>> call) {

        Function<Throwable, Boolean> condition = t -> {
            if (t instanceof NoStackTraceThrowable) {
                // retry request in case of Service Unavailable: Access Denied
                if (t.getMessage().startsWith("Service Unavailable:")) {
                    return true;
                }
            }

            return false;
        };

        return TestUtils.retry(vertx, call, condition);
    }


}
