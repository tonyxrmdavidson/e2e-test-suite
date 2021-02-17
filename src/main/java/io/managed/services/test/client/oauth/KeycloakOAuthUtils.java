package io.managed.services.test.client.oauth;

import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClientSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class KeycloakOAuthUtils {
    private static final Logger LOGGER = LogManager.getLogger(KeycloakOAuthUtils.class);

    public static Future<HttpResponse<Buffer>> startLogin(WebClientSession session, String authURI) {
        LOGGER.info("start oauth login; uri={}", authURI);
        Promise<HttpResponse<Buffer>> p = Promise.promise();
        session
                .getAbs(authURI)
                .send(p);
        return p.future();
    }

    public static Future<HttpResponse<Buffer>> postUsernamePassword(
            WebClientSession session, HttpResponse<Buffer> response, String username, String password) {

        Document d = Jsoup.parse(response.bodyAsString());
        String actionURI = d.select("#kc-form-login").attr("action");

        MultiMap f = MultiMap.caseInsensitiveMultiMap();
        f.add("username", username);
        f.add("password", password);

        LOGGER.info("post username and password; uri={}; username={}", actionURI, username);
        Promise<HttpResponse<Buffer>> p = Promise.promise();
        session
                .postAbs(actionURI)
                .sendForm(f, p);
        return p.future();
    }
}
