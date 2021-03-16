package io.managed.services.test;

import io.managed.services.test.client.ResponseException;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.HttpURLConnection;

@Tag(TestTag.SERVICE_API_PERMISSIONS)
@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ServiceAPUnauthorizedAndUnauthenticatedUserTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPISameOrgUserPermissionsTest.class);

    static final String FAKE_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICItNGVsY19WZE5fV3NPVVlmMkc0UXhyOEdjd0l4X0t0WFVDaXRhdExLbEx3In0.eyJleHAiOjE2MTM3MzI2NzAsImlhdCI6MTYxMzczMTc3MCwiYXV0aF90aW1lIjoxNjEzNzMxNzY5LCJqdGkiOiIyZjAzYjI4Ni0yNWEzLTQyZjItOTdlYS0zMjAwMjBjNWRkMzYiLCJpc3MiOiJodHRwczovL3Nzby5yZWRoYXQuY29tL2F1dGgvcmVhbG1zL3JlZGhhdC1leHRlcm5hbCIsImF1ZCI6ImNsb3VkLXNlcnZpY2VzIiwic3ViIjoiZjo1MjhkNzZmZi1mNzA4LTQzZWQtOGNkNS1mZTE2ZjRmZTBjZTY6bWstdGVzdC11c2VyLWUyZS1wcmltYXJ5IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiY2xvdWQtc2VydmljZXMiLCJzZXNzaW9uX3N0YXRlIjoiNWIzNzMzODktM2FhOC00YjExLTg2MTctOGYwNDQwM2Y2OTE5IiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyJodHRwczovL3Byb2QuZm9vLnJlZGhhdC5jb206MTMzNyIsImh0dHBzOi8vYXBpLmNsb3VkLnJlZGhhdC5jb20iLCJodHRwczovL3FhcHJvZGF1dGguY2xvdWQucmVkaGF0LmNvbSIsImh0dHBzOi8vY2xvdWQub3BlbnNoaWZ0LmNvbSIsImh0dHBzOi8vcHJvZC5mb28ucmVkaGF0LmNvbSIsImh0dHBzOi8vY2xvdWQucmVkaGF0LmNvbSJdLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsiYXV0aGVudGljYXRlZCJdfSwic2NvcGUiOiIiLCJhY2NvdW50X251bWJlciI6IjcwMjQ0MDciLCJpc19pbnRlcm5hbCI6ZmFsc2UsImFjY291bnRfaWQiOiI1Mzk4ODU3NCIsImlzX2FjdGl2ZSI6dHJ1ZSwib3JnX2lkIjoiMTQwMTQxNjEiLCJsYXN0X25hbWUiOiJVc2VyIiwidHlwZSI6IlVzZXIiLCJsb2NhbGUiOiJlbl9VUyIsImZpcnN0X25hbWUiOiJUZXN0IiwiZW1haWwiOiJtay10ZXN0LXVzZXIrZTJlLXByaW1hcnlAcmVkaGF0LmNvbSIsInVzZXJuYW1lIjoibWstdGVzdC11c2VyLWUyZS1wcmltYXJ5IiwiaXNfb3JnX2FkbWluIjpmYWxzZX0.y0OHnHA8wLKPhpoeBp_8V4r76R6Miqdj6fNevWHOBsrJ4_j9GJJ2QfJmeTSY5V3d0nT2Rt2SZ9trPrLEFd3Wr5z9YGIle--TXKKkYKyyFr4FO8Uaxvh-oN45C3cGsNYfbRBILqBCFHTmh54q1XoHA6FiteqdgMzUrBAoFG3SeFLl41u9abNA7EEe80ldozXsiSaLDWSylF1g9u1BhGqGuOpX0RoZGuTL_3KINEE7XoCbvW0xKecCA8-u1C06X_GUgR0tVvdgoGpB9uPDX3sbqMpl7fNgJvwyZa8acVoJuxs5K945OYGzGXuDGGzt-zxEov9g4udCDxNQTUoHuCIrMrr1ubt2iFbqso4UF6h-NIbxqARxhlhhyH8U9c2Zm1J_fLA9WJ8g1DJF75D66hV05s_RyRX1G6dFEriuT00PbGZQrxgH38zgZ8s-aS3qCAc2vYS-ZD4_Sl2xQgICC1HYpbgUbWNeAVEOWygZJUPMJLgpJ3aM2P8Dnjia50KL0owSTYBWvFDkROI-ymDXfcRvEMVKyOdhljQNPZew4Ux4apBi9t-ncB9XabDo11eddbbmcV05FWDb8X4opshptnWDzAw4ZPhbjoTBhNEI2JbFssOSYpskNnkB4kKQbBjVxAPldBNFwRKLOfvJNdY1jNurMY1xVMl2dbEpFBkqJf1lByU";

    @Test
    void testUnauthorizedUser(Vertx vertx, VertxTestContext context) {
        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_UNAUTHORIZED_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        ServiceAPIUtils.serviceAPI(vertx, Environment.SSO_UNAUTHORIZED_USERNAME, Environment.SSO_UNAUTHORIZED_PASSWORD)

                .compose(api -> api.getListOfKafkas())
                .compose(r -> Future.failedFuture("Get kafka list initially should fail!"))
                .recover(throwable -> {
                    if (throwable instanceof ResponseException && ((ResponseException) throwable).response.statusCode() == HttpURLConnection.HTTP_FORBIDDEN) {
                        LOGGER.info("{} is unauthorized user", Environment.SSO_UNAUTHORIZED_USERNAME);
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })

                .onComplete(context.succeedingThenComplete());
    }

    @Test
    void testUnauthenticatedUserWithFakeToken(Vertx vertx, VertxTestContext context) {
        var api = new ServiceAPI(vertx, Environment.SERVICE_API_URI, FAKE_TOKEN);

        api.getListOfKafkas()
                .compose(r -> Future.failedFuture("Get kafka list initially should fail!"))
                .recover(throwable -> {
                    if (throwable instanceof ResponseException && ((ResponseException) throwable).response.statusCode() == HttpURLConnection.HTTP_UNAUTHORIZED) {
                        LOGGER.info("{} is unauthenticated user", Environment.SSO_UNAUTHORIZED_USERNAME);
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })

                .onComplete(context.succeedingThenComplete());
    }

    @Test
    void testUnauthenticatedUserWithoutToken(Vertx vertx, VertxTestContext context) {
        var api = new ServiceAPI(vertx, Environment.SERVICE_API_URI, "");

        api.getListOfKafkas()
                .compose(r -> Future.failedFuture("Get kafka list initially should fail!"))
                .recover(throwable -> {
                    if (throwable instanceof ResponseException && ((ResponseException) throwable).response.statusCode() == HttpURLConnection.HTTP_UNAUTHORIZED) {
                        LOGGER.info("{} is unauthenticated user", Environment.SSO_UNAUTHORIZED_USERNAME);
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(throwable);
                })

                .onComplete(context.succeedingThenComplete());
    }

}
