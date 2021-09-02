package io.managed.services.test.client.securitymgmt;


import com.openshift.cloud.api.kas.invoker.ApiClient;
import com.openshift.cloud.api.kas.invoker.auth.HttpBearerAuth;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import com.openshift.cloud.api.kas.models.ServiceAccountListItem;
import com.openshift.cloud.api.kas.models.ServiceAccountRequest;
import io.managed.services.test.Environment;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.User;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;


public class SecurityMgmtAPIUtils {
    private static final Logger LOGGER = LogManager.getLogger(SecurityMgmtAPIUtils.class);

    public static Future<SecurityMgmtApi> securityMgmtApi(Vertx vertx) {
        return securityMgmtApi(vertx, Environment.SSO_USERNAME, Environment.SSO_PASSWORD);
    }

    public static Future<SecurityMgmtApi> securityMgmtApi(Vertx vertx, String username, String password) {
        var auth = new KeycloakOAuth(vertx);

        LOGGER.info("authenticate user: {} against RH SSO", username);
        return auth.loginToRHSSO(username, password)
            .map(u -> securityMgmtApi(u));
    }

    public static SecurityMgmtApi securityMgmtApi(User user) {
        return securityMgmtApi(Environment.SERVICE_API_URI, KeycloakOAuth.getToken(user));
    }

    public static SecurityMgmtApi securityMgmtApi(String uri, String token) {
        var apiClient = new ApiClient();
        apiClient.setBasePath(uri);
        ((HttpBearerAuth) apiClient.getAuthentication("Bearer")).setBearerToken(token);
        LOGGER.info("token: {}", token);
        return new SecurityMgmtApi(apiClient);
    }

    /**
     * Get Service Account by name or return empty optional
     *
     * @param api  SecurityMgmtApi
     * @param name Service Account name
     * @return Optional ServiceAccount
     */
    public static Optional<ServiceAccountListItem> getServiceAccountByName(SecurityMgmtApi api, String name)
        throws ApiGenericException {

        var list = api.getServiceAccounts();
        return list.getItems().stream().filter(a -> name.equals(a.getName())).findAny();
    }

    public static void cleanServiceAccount(SecurityMgmtApi api, String name)
        throws ApiGenericException {

        deleteServiceAccountByNameIfExists(api, name);
    }

    /**
     * Delete Service Account by name if it exists
     *
     * @param api  SecurityMgmtApi
     * @param name Service Account name
     */
    public static void deleteServiceAccountByNameIfExists(SecurityMgmtApi api, String name)
        throws ApiGenericException {

        var exists = getServiceAccountByName(api, name);
        if (exists.isPresent()) {
            var serviceAccount = exists.get();
            LOGGER.info("delete service account '{}'", serviceAccount.getName());
            LOGGER.debug(serviceAccount);
            api.deleteServiceAccountById(serviceAccount.getId());
            LOGGER.info("service account '{}' deleted", serviceAccount.getName());
        } else {
            LOGGER.info("service account '{}' not found", name);
        }
    }

    /**
     * If the service account with the passed name doesn't exists, recreate it, otherwise reset the credentials
     * and return the ServiceAccount with clientSecret
     *
     * @param api  SecurityMgmtApi
     * @param name Service Account Name
     * @return ServiceAccount with clientSecret
     */
    public static ServiceAccount applyServiceAccount(SecurityMgmtApi api, String name)
        throws ApiGenericException {

        var existing = getServiceAccountByName(api, name);

        ServiceAccount serviceAccount;
        if (existing.isPresent()) {
            LOGGER.warn("reset service account '{}' credentials", existing.get().getName());
            serviceAccount = api.resetServiceAccountCreds(existing.get().getId());
            LOGGER.debug(serviceAccount);
        } else {
            LOGGER.info("create service account '{}'", name);
            serviceAccount = api.createServiceAccount(new ServiceAccountRequest().name(name));
        }
        return serviceAccount;
    }
}
