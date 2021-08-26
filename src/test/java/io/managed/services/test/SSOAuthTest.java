package io.managed.services.test;

import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;

import static io.managed.services.test.TestUtils.bwait;


@Test(groups = TestTag.SERVICE_API)
public class SSOAuthTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(SSOAuthTest.class);

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testRedHatSSOLogin() throws Throwable {
        var auth = new KeycloakOAuth(Vertx.vertx());

        bwait(auth.loginToRHSSO(Environment.SSO_USERNAME, Environment.SSO_PASSWORD));
        LOGGER.info("user authenticated against: {}", Environment.SSO_REDHAT_KEYCLOAK_URI);
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testMASSSOLogin() throws Throwable {
        var auth2 = new KeycloakOAuth(Vertx.vertx());

        bwait(auth2.loginToMASSSO(Environment.SSO_USERNAME, Environment.SSO_PASSWORD));
        LOGGER.info("user authenticated against: {}", Environment.MAS_SSO_REDHAT_KEYCLOAK_URI);
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testJoinedLogin() throws Throwable {
        var auth = new KeycloakOAuth(Vertx.vertx());

        bwait(auth.loginToRHSSO(Environment.SSO_USERNAME, Environment.SSO_PASSWORD));
        LOGGER.info("user authenticated against: {}", Environment.SSO_REDHAT_KEYCLOAK_URI);

        bwait(auth.loginToMASSSO(Environment.SSO_USERNAME, Environment.SSO_PASSWORD));
        LOGGER.info("user authenticated against: {}", Environment.MAS_SSO_REDHAT_KEYCLOAK_URI);
    }
}
