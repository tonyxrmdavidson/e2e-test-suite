package io.managed.services.test.quickstarts.contexts;

import com.openshift.cloud.api.kas.models.ServiceAccount;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
public class ServiceAccountContext {
    private ServiceAccount serviceAccount;

    public ServiceAccount requireServiceAccount() {
        return Objects.requireNonNull(serviceAccount);
    }
}
