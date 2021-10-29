package io.managed.services.test.quickstarts.contexts;

import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
public class OpenShiftAPIContext {
    private KafkaMgmtApi kafkaMgmtApi;
    private SecurityMgmtApi securityMgmtApi;

    public KafkaMgmtApi requireKafkaMgmtApi() {
        return Objects.requireNonNull(kafkaMgmtApi);
    }

    public SecurityMgmtApi requireSecurityMgmtApi() {
        return Objects.requireNonNull(securityMgmtApi);
    }
}
