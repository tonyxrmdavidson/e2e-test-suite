package io.managed.services.test.client.registrymgmt;
import com.openshift.cloud.api.srs.models.Registry;

public class RegistryNotReadyException extends Exception {

    public RegistryNotReadyException(Registry r, Exception cause) {
        super("registry not ready: \n" + r.toString(), cause);
    }
}
