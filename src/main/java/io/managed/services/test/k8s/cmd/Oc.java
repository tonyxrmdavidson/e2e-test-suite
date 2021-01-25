package io.managed.services.test.k8s.cmd;

import io.managed.services.test.executor.Exec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class Oc extends BaseCmdKubeClient<Oc> {

    private static final Logger LOGGER = LogManager.getLogger(Oc.class);

    private static final String OC = "oc";

    public Oc(String config) {
        super(config);
    }

    private Oc(String futureNamespace, String config) {
        this(config);
        namespace = futureNamespace;
    }

    @Override
    public String defaultNamespace() {
        return "default";
    }

    @Override
    public Oc namespace(String namespace) {
        return new Oc(namespace, config);
    }

    @Override
    public String namespace() {
        return namespace;
    }

    @Override
    public Oc createNamespace(String name) {
        Exec.builder()
                .withCommand(cmd(), "--kubeconfig", config, "new-project", name)
                .exec();
        return this;
    }

    public Oc newApp(String template, Map<String, String> params) {
        List<String> cmd = namespacedCommand("new-app", template);
        for (Map.Entry<String, String> entry : params.entrySet()) {
            cmd.add("-p");
            cmd.add(entry.getKey() + "=" + entry.getValue());
        }

        Exec.builder()
                .withCommand(cmd)
                .exec();
        return this;
    }

    @Override
    public String cmd() {
        return OC;
    }
}
