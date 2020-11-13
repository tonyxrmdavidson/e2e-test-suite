package io.managed.services.test.k8s.cluster;

import io.managed.services.test.executor.Exec;
import io.managed.services.test.k8s.cmdClient.KubeCmdClient;
import io.managed.services.test.k8s.cmdClient.Oc;
import io.managed.services.test.k8s.exceptions.KubeClusterException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OpenShift implements KubeCluster {

    private static final String OC = "oc";
    private static final String OLM_NAMESPACE = "openshift-operators";
    private static final Logger LOGGER = LogManager.getLogger(OpenShift.class);

    @Override
    public boolean isAvailable() {
        return Exec.isExecutableOnPath(OC);
    }

    @Override
    public boolean isClusterUp() {
        try {
            return Exec.exec(OC, "status").exitStatus() && Exec.exec(OC, "api-resources").out().contains("openshift.io");
        } catch (KubeClusterException e) {
            LOGGER.debug("Error:", e);
            return false;
        }
    }

    @Override
    public KubeCmdClient defaultCmdClient() {
        return new Oc();
    }

    public String toString() {
        return OC;
    }

    @Override
    public String defaultOlmNamespace() {
        return OLM_NAMESPACE;
    }
}
