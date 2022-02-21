package io.managed.services.test;

import io.managed.services.test.client.oauth.KeycloakLoginSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;


/**
 * Test Login to Red Hat SSO and Openshift Identity.
 * <p>
 * <b>Requires:</b>
 * <ul>
 *     <li> PRIMARY_USERNAME
 *     <li> PRIMARY_PASSWORD
 * </ul>
 */
public class SSOAuthTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(SSOAuthTest.class);

    @Test
    public void testRedHatSSOLogin() throws Throwable {
        var auth = KeycloakLoginSession.primaryUser();
        auth.loginToRedHatSSO();
        LOGGER.info("user authenticated against: {}", Environment.REDHAT_SSO_URI);
    }

    @Test
    public void testMASSSOLogin() throws Throwable {
        var auth2 = KeycloakLoginSession.primaryUser();
        auth2.loginToOpenshiftIdentity();
        LOGGER.info("user authenticated against: {}", Environment.OPENSHIFT_IDENTITY_URI);
    }

    @Test
    public void testJoinedLogin() throws Throwable {
        var auth = KeycloakLoginSession.primaryUser();

        auth.loginToRedHatSSO();
        LOGGER.info("user authenticated against: {}", Environment.REDHAT_SSO_URI);

        auth.loginToOpenshiftIdentity();
        LOGGER.info("user authenticated against: {}", Environment.OPENSHIFT_IDENTITY_URI);
    }
}
