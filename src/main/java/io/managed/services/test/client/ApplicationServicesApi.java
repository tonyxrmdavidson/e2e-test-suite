package io.managed.services.test.client;

import io.managed.services.test.Environment;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.oauth.KeycloakLoginSession;
import io.managed.services.test.client.oauth.KeycloakUser;
import io.managed.services.test.client.registrymgmt.RegistryMgmtApi;
import io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import static io.managed.services.test.TestUtils.bwait;

@Log4j2
@Deprecated
public class ApplicationServicesApi {

    private final KafkaMgmtApi kafkaMgmtApi;
    private final SecurityMgmtApi securityMgmtApi;
    private final RegistryMgmtApi registryMgmtApi;

    public ApplicationServicesApi(String basePath, KeycloakUser user) {
        this.kafkaMgmtApi = KafkaMgmtApiUtils.kafkaMgmtApi(basePath, user);
        this.securityMgmtApi = SecurityMgmtAPIUtils.securityMgmtApi(basePath, user);
        this.registryMgmtApi = RegistryMgmtApiUtils.registryMgmtApi(basePath, user);
    }

    public static ApplicationServicesApi applicationServicesApi(String username, String password) {
        return applicationServicesApi(Environment.OPENSHIFT_API_URI, username, password);
    }

    public static ApplicationServicesApi applicationServicesApi(KeycloakLoginSession auth) {
        return applicationServicesApi(auth, Environment.OPENSHIFT_API_URI);
    }

    public static ApplicationServicesApi applicationServicesApi(String basePath, String username, String password) {
        return applicationServicesApi(new KeycloakLoginSession(username, password), basePath);
    }

    @SneakyThrows
    public static ApplicationServicesApi applicationServicesApi(KeycloakLoginSession auth, String basePath) {

        log.info("authenticate user '{}' against RH SSO", auth.getUsername());
        var user = bwait(auth.loginToRedHatSSO());
        return new ApplicationServicesApi(basePath, user);
    }

    public KafkaMgmtApi kafkaMgmt() {
        return kafkaMgmtApi;
    }

    public SecurityMgmtApi securityMgmt() {
        return securityMgmtApi;
    }

    public RegistryMgmtApi registryMgmt() {
        return registryMgmtApi;
    }
}
