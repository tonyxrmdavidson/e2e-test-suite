package io.managed.services.test.k8s.cluster;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.utils.HttpClientUtils;
import io.fabric8.openshift.client.DefaultOpenShiftClient;
import io.fabric8.openshift.client.OpenShiftConfig;
import io.managed.services.test.k8s.KubeClient;
import io.managed.services.test.k8s.cmd.KubeCmdClient;
import io.managed.services.test.k8s.cmd.Oc;
import okhttp3.OkHttpClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class OpenShift implements KubeCluster {

    private static final String OC = "oc";
    private static final Logger LOGGER = LogManager.getLogger(OpenShift.class);


    @Override
    public KubeCmdClient defaultCmdClient(String config) {
        return new Oc(config);
    }

    @Override
    public KubeClient defaultClient(String kubeconfig) throws IOException {
        Config config = getConfig(kubeconfig);
        OkHttpClient client = HttpClientUtils.createHttpClient(config).newBuilder()
                .connectTimeout(30_000, TimeUnit.MILLISECONDS)
                .build();
        return new KubeClient(new DefaultOpenShiftClient(client, new OpenShiftConfig(config)), "default");
    }

    public String toString() {
        return OC;
    }
}
