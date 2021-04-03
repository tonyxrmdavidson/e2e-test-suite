package io.managed.services.test;

import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.TimeUnit;


@Tag(TestTag.SERVICE_API)
@ExtendWith(VertxExtension.class)
@Timeout(value = 5, timeUnit = TimeUnit.MINUTES)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SSOAuthTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(SSOAuthTest.class);

    @Test
    void testRedHatSSOLogin(Vertx vertx, VertxTestContext context) {

        var auth = new KeycloakOAuth(vertx);

        auth.login(
                Environment.SSO_REDHAT_KEYCLOAK_URI,
                Environment.SSO_REDHAT_REDIRECT_URI,
                Environment.SSO_REDHAT_REALM,
                Environment.SSO_REDHAT_CLIENT_ID,
                Environment.SSO_USERNAME,
                Environment.SSO_PASSWORD)
                .onSuccess(user -> LOGGER.info("user authenticated against: {}", Environment.SSO_REDHAT_KEYCLOAK_URI))

                .onComplete(context.succeedingThenComplete());
    }

    @Test
    void testMASSSOLogin(Vertx vertx, VertxTestContext context) {
        var auth2 = new KeycloakOAuth(vertx);

        auth2.login(
                Environment.MAS_SSO_REDHAT_KEYCLOAK_URI,
                Environment.MAS_SSO_REDHAT_REDIRECT_URI,
                Environment.MAS_SSO_REDHAT_REALM,
                Environment.MAS_SSO_REDHAT_CLIENT_ID,
                Environment.SSO_USERNAME,
                Environment.SSO_PASSWORD)
                .onSuccess(user -> LOGGER.info("user authenticated against: {}", Environment.MAS_SSO_REDHAT_KEYCLOAK_URI))

                .onComplete(context.succeedingThenComplete());
    }

    @Test
    void testJoinedLogin(Vertx vertx, VertxTestContext context) {
        var auth = new KeycloakOAuth(vertx);

        auth.login(
                Environment.SSO_REDHAT_KEYCLOAK_URI,
                Environment.SSO_REDHAT_REDIRECT_URI,
                Environment.SSO_REDHAT_REALM,
                Environment.SSO_REDHAT_CLIENT_ID,
                Environment.SSO_USERNAME,
                Environment.SSO_PASSWORD)
                .onSuccess(user -> LOGGER.info("user authenticated against: {}", Environment.SSO_REDHAT_KEYCLOAK_URI))

                .compose(__ -> auth.login(
                        Environment.MAS_SSO_REDHAT_KEYCLOAK_URI,
                        Environment.MAS_SSO_REDHAT_REDIRECT_URI,
                        Environment.MAS_SSO_REDHAT_REALM,
                        Environment.MAS_SSO_REDHAT_CLIENT_ID))
                .onSuccess(user -> LOGGER.info("user authenticated against: {}", Environment.MAS_SSO_REDHAT_KEYCLOAK_URI))

                .onComplete(context.succeedingThenComplete());
    }
}
