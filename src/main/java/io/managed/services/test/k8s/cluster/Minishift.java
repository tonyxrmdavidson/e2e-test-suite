package io.managed.services.test.k8s.cluster;

import io.managed.services.test.executor.Exec;
import io.managed.services.test.k8s.cmdClient.KubeCmdClient;
import io.managed.services.test.k8s.cmdClient.Oc;
import io.managed.services.test.k8s.exceptions.KubeClusterException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Minishift implements KubeCluster {

    private static final String CMD = "minishift";
    private static final String OLM_NAMESPACE = "openshift-operators";
    private static final Logger LOGGER = LogManager.getLogger(Minishift.class);

    @Override
    public boolean isAvailable() {
        return Exec.isExecutableOnPath(CMD);
    }

    @Override
    public boolean isClusterUp() {
        try {
            String output = Exec.exec(CMD, "status").out();
            return output.contains("Minishift:  Running")
                    && output.contains("OpenShift:  Running");
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
        return CMD;
    }

    @Override
    public String defaultOlmNamespace() {
        return OLM_NAMESPACE;
    }
}
