package io.managed.services.test.client.oauth;

import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthConstants;
import com.github.scribejava.core.oauth.AccessTokenRequestParams;
import com.github.scribejava.core.oauth.OAuth20Service;
import io.managed.services.test.RetryUtils;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.ext.web.client.HttpResponse;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.internal.StringUtil;
import org.jsoup.nodes.FormElement;

import java.util.function.Function;
import java.util.function.Supplier;

import static io.managed.services.test.client.BaseVertxClient.getRedirectLocation;
import static io.managed.services.test.client.BaseVertxClient.getRedirectLocationAsFeature;

public class KeycloakLoginUtils {
    private static final Logger LOGGER = LogManager.getLogger(KeycloakLoginUtils.class);

    public static Future<VertxHttpResponse> followRedirects(
        VertxWebClientSession session, VertxHttpResponse response) {

        var c = response.statusCode();
        if (c >= 300 && c < 400) {

            // handle redirects
            return getRedirectLocationAsFeature(response)
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

    @SneakyThrows
    public static OAuth2AccessToken authenticateUser(OAuth20Service oauth2, HttpResponse<Buffer> response) {

        var redirectLocation = getRedirectLocation(response);
        LOGGER.info("redirect location found: {}", redirectLocation);
        var auth = oauth2.extractAuthorization(redirectLocation);

        var params = AccessTokenRequestParams.create(auth.getCode())
            .addExtraParameter(OAuthConstants.CLIENT_ID, oauth2.getApiKey());
        return oauth2.getAccessToken(auth.getCode());
    }

    @Deprecated
    private static <T> Future<T> retry(Vertx vertx, Supplier<Future<T>> call) {

        Function<Throwable, Boolean> condition = t -> {
            if (t instanceof NoStackTraceThrowable) {
                // retry request in case of Service Unavailable: Access Denied
                return t.getMessage().startsWith("Service Unavailable:");
            }

            return false;
        };

        return RetryUtils.retry(vertx, 1, call, condition);
    }
}
