package io.managed.services.test;

import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static io.managed.services.test.TestUtils.bwait;


@Tag(TestTag.SERVICE_API)
@Timeout(value = 5, unit = TimeUnit.MINUTES)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SSOAuthTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(SSOAuthTest.class);

    @Test
    void testRedHatSSOLogin() throws Throwable {

        var auth = new KeycloakOAuth(Vertx.vertx());

        bwait(auth.login(
            Environment.SSO_REDHAT_KEYCLOAK_URI,
            Environment.SSO_REDHAT_REDIRECT_URI,
            Environment.SSO_REDHAT_REALM,
            Environment.SSO_REDHAT_CLIENT_ID,
            Environment.SSO_USERNAME,
            Environment.SSO_PASSWORD));
        LOGGER.info("user authenticated against: {}", Environment.SSO_REDHAT_KEYCLOAK_URI);
    }

    @Test
    void testMASSSOLogin() throws Throwable {
        var auth2 = new KeycloakOAuth(Vertx.vertx());

        bwait(auth2.login(
            Environment.MAS_SSO_REDHAT_KEYCLOAK_URI,
            Environment.MAS_SSO_REDHAT_REDIRECT_URI,
            Environment.MAS_SSO_REDHAT_REALM,
            Environment.MAS_SSO_REDHAT_CLIENT_ID,
            Environment.SSO_USERNAME,
            Environment.SSO_PASSWORD));
        LOGGER.info("user authenticated against: {}", Environment.MAS_SSO_REDHAT_KEYCLOAK_URI);
    }

    @Test
    void testJoinedLogin() throws Throwable {
        var auth = new KeycloakOAuth(Vertx.vertx());

        bwait(auth.login(
            Environment.SSO_REDHAT_KEYCLOAK_URI,
            Environment.SSO_REDHAT_REDIRECT_URI,
            Environment.SSO_REDHAT_REALM,
            Environment.SSO_REDHAT_CLIENT_ID,
            Environment.SSO_USERNAME,
            Environment.SSO_PASSWORD));
        LOGGER.info("user authenticated against: {}", Environment.SSO_REDHAT_KEYCLOAK_URI);

        bwait(auth.login(
            Environment.MAS_SSO_REDHAT_KEYCLOAK_URI,
            Environment.MAS_SSO_REDHAT_REDIRECT_URI,
            Environment.MAS_SSO_REDHAT_REALM,
            Environment.MAS_SSO_REDHAT_CLIENT_ID,
            Environment.SSO_USERNAME,
            Environment.SSO_PASSWORD));
        LOGGER.info("user authenticated against: {}", Environment.MAS_SSO_REDHAT_KEYCLOAK_URI);
    }
}
