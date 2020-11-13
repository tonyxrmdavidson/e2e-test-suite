package io.managed.services.test.k8s.cmdClient;

/**
 * A {@link KubeCmdClient} wrapping {@code kubectl}.
 */
public class Kubectl extends BaseCmdKubeClient<Kubectl> {

    public static final String KUBECTL = "kubectl";

    public Kubectl() { }

    Kubectl(String futureNamespace) {
        namespace = futureNamespace;
    }

    @Override
    public Kubectl namespace(String namespace) {
        return new Kubectl(namespace);
    }

    @Override
    public String namespace() {
        return namespace;
    }

    @Override
    public String defaultNamespace() {
        return "default";
    }

    @Override
    public String defaultOlmNamespace() {
        return "operators";
    }

    @Override
    public String cmd() {
        return KUBECTL;
    }

    @Override
    public Kubectl clientWithAdmin() {
        return this;
    }


}
