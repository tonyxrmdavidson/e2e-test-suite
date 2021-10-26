package io.managed.services.test.client.oauth;

import com.github.scribejava.apis.KeycloakApi;
import com.github.scribejava.core.oauth2.clientauthentication.ClientAuthentication;
import com.github.scribejava.core.oauth2.clientauthentication.RequestBodyAuthenticationScheme;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Keycloak2Api extends KeycloakApi {

    private static final ConcurrentMap<String, Keycloak2Api> INSTANCES = new ConcurrentHashMap<>();

    protected Keycloak2Api(String baseUrlWithRealm) {
        super(baseUrlWithRealm);
    }

    public static KeycloakApi instance(String baseUrl, String realm) {
        var defaultBaseUrlWithRealm = composeBaseUrlWithRealm(baseUrl, realm);
        var api = INSTANCES.get(defaultBaseUrlWithRealm);
        if (api == null) {
            api = new Keycloak2Api(defaultBaseUrlWithRealm);
            var alreadyCreatedApi = INSTANCES.putIfAbsent(defaultBaseUrlWithRealm, api);
            if (alreadyCreatedApi != null) {
                return alreadyCreatedApi;
            }
        }
        return api;
    }

    @Override
    public ClientAuthentication getClientAuthentication() {
        return RequestBodyAuthenticationScheme.instance();
    }
}
