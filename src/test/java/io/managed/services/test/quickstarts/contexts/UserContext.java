package io.managed.services.test.quickstarts.contexts;

import io.managed.services.test.client.oauth.KeycloakLoginSession;
import io.managed.services.test.client.oauth.KeycloakUser;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Setter
@Getter
public class UserContext {
    private KeycloakLoginSession keycloakLoginSession;
    private KeycloakUser redHatUser;
    private KeycloakUser masUser;

    public KeycloakUser requireMasUser() {
        return Objects.requireNonNull(masUser);
    }
}
