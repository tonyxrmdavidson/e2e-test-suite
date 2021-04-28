package io.managed.services.test;

import io.managed.services.test.client.exception.HTTPForbiddenException;
import io.managed.services.test.client.exception.HTTPUnauthorizedException;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
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
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(TestTag.SERVICE_API_PERMISSIONS)
@Timeout(value = 5, unit = TimeUnit.MINUTES)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ServiceAPIUnauthUserTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPIUnauthUserTest.class);

    private final Vertx vertx = Vertx.vertx();

    static final String FAKE_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUI" +
        "iwia2lkIiA6ICItNGVsY19WZE5fV3NPVVlmMkc0UXhyOEdjd0l4X0t0WFVDaXRhd" +
        "ExLbEx3In0.eyJleHAiOjE2MTM3MzI2NzAsImlhdCI6MTYxMzczMTc3MCwiYXV0a" +
        "F90aW1lIjoxNjEzNzMxNzY5LCJqdGkiOiIyZjAzYjI4Ni0yNWEzLTQyZjItOTdlY" +
        "S0zMjAwMjBjNWRkMzYiLCJpc3MiOiJodHRwczovL3Nzby5yZWRoYXQuY29tL2F1d" +
        "GgvcmVhbG1zL3JlZGhhdC1leHRlcm5hbCIsImF1ZCI6ImNsb3VkLXNlcnZpY2VzI" +
        "iwic3ViIjoiZjo1MjhkNzZmZi1mNzA4LTQzZWQtOGNkNS1mZTE2ZjRmZTBjZTY6b" +
        "WstdGVzdC11c2VyLWUyZS1wcmltYXJ5IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiY" +
        "2xvdWQtc2VydmljZXMiLCJzZXNzaW9uX3N0YXRlIjoiNWIzNzMzODktM2FhOC00Y" +
        "jExLTg2MTctOGYwNDQwM2Y2OTE5IiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2luc" +
        "yI6WyJodHRwczovL3Byb2QuZm9vLnJlZGhhdC5jb206MTMzNyIsImh0dHBzOi8vY" +
        "XBpLmNsb3VkLnJlZGhhdC5jb20iLCJodHRwczovL3FhcHJvZGF1dGguY2xvdWQuc" +
        "mVkaGF0LmNvbSIsImh0dHBzOi8vY2xvdWQub3BlbnNoaWZ0LmNvbSIsImh0dHBzO" +
        "i8vcHJvZC5mb28ucmVkaGF0LmNvbSIsImh0dHBzOi8vY2xvdWQucmVkaGF0LmNvb" +
        "SJdLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsiYXV0aGVudGljYXRlZCJdfSwic" +
        "2NvcGUiOiIiLCJhY2NvdW50X251bWJlciI6IjcwMjQ0MDciLCJpc19pbnRlcm5hb" +
        "CI6ZmFsc2UsImFjY291bnRfaWQiOiI1Mzk4ODU3NCIsImlzX2FjdGl2ZSI6dHJ1Z" +
        "Swib3JnX2lkIjoiMTQwMTQxNjEiLCJsYXN0X25hbWUiOiJVc2VyIiwidHlwZSI6I" +
        "lVzZXIiLCJsb2NhbGUiOiJlbl9VUyIsImZpcnN0X25hbWUiOiJUZXN0IiwiZW1ha" +
        "WwiOiJtay10ZXN0LXVzZXIrZTJlLXByaW1hcnlAcmVkaGF0LmNvbSIsInVzZXJuY" +
        "W1lIjoibWstdGVzdC11c2VyLWUyZS1wcmltYXJ5IiwiaXNfb3JnX2FkbWluIjpmY" +
        "WxzZX0.y0OHnHA8wLKPhpoeBp_8V4r76R6Miqdj6fNevWHOBsrJ4_j9GJJ2QfJme" +
        "TSY5V3d0nT2Rt2SZ9trPrLEFd3Wr5z9YGIle--TXKKkYKyyFr4FO8Uaxvh-oN45C" +
        "3cGsNYfbRBILqBCFHTmh54q1XoHA6FiteqdgMzUrBAoFG3SeFLl41u9abNA7EEe8" +
        "0ldozXsiSaLDWSylF1g9u1BhGqGuOpX0RoZGuTL_3KINEE7XoCbvW0xKecCA8-u1" +
        "C06X_GUgR0tVvdgoGpB9uPDX3sbqMpl7fNgJvwyZa8acVoJuxs5K945OYGzGXuDG" +
        "Gzt-zxEov9g4udCDxNQTUoHuCIrMrr1ubt2iFbqso4UF6h-NIbxqARxhlhhyH8U9" +
        "c2Zm1J_fLA9WJ8g1DJF75D66hV05s_RyRX1G6dFEriuT00PbGZQrxgH38zgZ8s-a" +
        "S3qCAc2vYS-ZD4_Sl2xQgICC1HYpbgUbWNeAVEOWygZJUPMJLgpJ3aM2P8Dnjia5" +
        "0KL0owSTYBWvFDkROI-ymDXfcRvEMVKyOdhljQNPZew4Ux4apBi9t-ncB9XabDo1" +
        "1eddbbmcV05FWDb8X4opshptnWDzAw4ZPhbjoTBhNEI2JbFssOSYpskNnkB4kKQb" +
        "BjVxAPldBNFwRKLOfvJNdY1jNurMY1xVMl2dbEpFBkqJf1lByU";

    @Test
    void testUnauthorizedUser() throws Throwable {
        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_UNAUTHORIZED_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        var api = bwait(ServiceAPIUtils.serviceAPI(vertx, Environment.SSO_UNAUTHORIZED_USERNAME, Environment.SSO_UNAUTHORIZED_PASSWORD));
        assertThrows(HTTPForbiddenException.class, () -> bwait(api.getListOfKafkas()));
    }

    @Test
    public void testUnauthenticatedUserWithFakeToken() {
        var api = new ServiceAPI(vertx, Environment.SERVICE_API_URI, FAKE_TOKEN);
        assertThrows(HTTPUnauthorizedException.class, () -> bwait(api.getListOfKafkas()));
    }

    @Test
    public void testUnauthenticatedUserWithoutToken() {
        var api = new ServiceAPI(vertx, Environment.SERVICE_API_URI, "");
        assertThrows(HTTPUnauthorizedException.class, () -> bwait(api.getListOfKafkas()));
    }
}
