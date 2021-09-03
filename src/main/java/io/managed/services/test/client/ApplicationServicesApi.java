package io.managed.services.test.client;

import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.managed.services.test.client.registry.RegistriesApi;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
import io.vertx.ext.auth.User;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import static io.managed.services.test.TestUtils.bwait;

@Log4j2
public class ApplicationServicesApi {

    private final KafkaMgmtApi kafkaMgmtApi;
    private final SecurityMgmtApi securityMgmtApi;
    private final RegistriesApi registriesApi;

    public ApplicationServicesApi(String basePath, User user) {
        this(basePath, KeycloakOAuth.getToken(user));
    }

    public ApplicationServicesApi(String basePath, String token) {
        this(new KasApiClient().basePath(basePath).bearerToken(token),
            new SrsApiClient().basePath(basePath).bearerToken(token));
    }

    public ApplicationServicesApi(KasApiClient kasApiClient, SrsApiClient srsApiClient) {
        this.kafkaMgmtApi = new KafkaMgmtApi(kasApiClient.getApiClient());
        this.securityMgmtApi = new SecurityMgmtApi(kasApiClient.getApiClient());
        this.registriesApi = new RegistriesApi(srsApiClient.getApiClient());
    }

    @SneakyThrows
    public static ApplicationServicesApi applicationServicesApi(
        KeycloakOAuth auth, String basePath, String username, String password) {

        log.info("authenticate user '{}' against RH SSO", username);
        var user = bwait(auth.loginToRHSSO(username, password));
        return new ApplicationServicesApi(basePath, user);
    }

    public KafkaMgmtApi km() {
        return kafkaMgmtApi;
    }

    public SecurityMgmtApi sm() {
        return securityMgmtApi;
    }

    public RegistriesApi rm() {
        return registriesApi;
    }
}
