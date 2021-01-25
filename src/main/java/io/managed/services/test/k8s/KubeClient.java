package io.managed.services.test.k8s;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.Ingress;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.VersionInfo;
import io.fabric8.kubernetes.client.dsl.ExecListener;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.client.OpenShiftClient;
import io.managed.services.test.TestUtils;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Abstraction over fabric8 client
 */
public class KubeClient {

    private static final Logger LOGGER = LogManager.getLogger(KubeClient.class);
    protected final KubernetesClient client;
    protected String namespace;

    public KubeClient(KubernetesClient client, String namespace) {
        this.client = client;
        this.namespace = namespace;
    }

    /**
     * Returns fabric8 client for direc interaction
     *
     * @return fabric8 client
     */
    public KubernetesClient getClient() {
        return client;
    }

    /**
     * Set namespace of fabric8 client
     *
     * @param futureNamespace namespace name
     * @return kubernetes client
     */
    public KubeClient namespace(String futureNamespace) {
        return new KubeClient(this.client, futureNamespace);
    }

    public String getNamespace() {
        return namespace;
    }

    public Namespace getNamespace(String namespace) {
        return client.namespaces().withName(namespace).get();
    }

    public void createNamespace(String name) {
        createNamespace(name, Collections.emptyMap(), Collections.emptyMap());
    }

    public void createNamespace(String name, Map<String, String> annotations, Map<String, String> labels) {
        if (client.namespaces().withName(name).get() != null) {
            deleteNamespace(name);
            TestUtils.waitFor("Namespace " + name + " deleted", 1000, 240_000, () -> client.namespaces().withName(name).get() == null);
        }
        Namespace ns = new NamespaceBuilder().
                withNewMetadata()
                .withName(name)
                .withAnnotations(annotations.isEmpty() ? null : annotations)
                .withLabels(labels.isEmpty() ? null : labels)
                .endMetadata().build();
        client.namespaces().createOrReplace(ns);
    }

    public void deleteNamespace(String name) {
        client.namespaces().withName(name).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    /**
     * Gets namespace status
     */
    public boolean getNamespaceStatus(String namespaceName) {
        return client.namespaces().withName(namespaceName).isReady();
    }


    public void deleteConfigMap(String configMapName) {
        client.configMaps().inNamespace(getNamespace()).withName(configMapName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    public ConfigMap getConfigMap(String configMapName) {
        return client.configMaps().inNamespace(getNamespace()).withName(configMapName).get();
    }

    /**
     * Gets config map uid
     *
     * @param configMapName config map name
     * @return config map ui
     */
    public String getConfigMapUid(String configMapName) {
        return getConfigMap(configMapName).getMetadata().getUid();
    }

    public boolean getConfigMapStatus(String configMapName) {
        return client.configMaps().inNamespace(getNamespace()).withName(configMapName).isReady();
    }

    public List<ConfigMap> listConfigMaps() {
        return client.configMaps().inNamespace(getNamespace()).list().getItems();
    }

    public List<ConfigMap> listConfigMaps(String namePrefix) {
        return listConfigMaps().stream()
                .filter(cm -> cm.getMetadata().getName().startsWith(namePrefix))
                .collect(Collectors.toList());
    }

    public String execInPod(String podName, String container, String... command) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        LOGGER.info("Running command on pod {}: {}", podName, command);
        CountDownLatch execLatch = new CountDownLatch(1);

        try {
            client.pods().inNamespace(getNamespace())
                    .withName(podName).inContainer(container)
                    .readingInput(null)
                    .writingOutput(baos)
                    .usingListener(new SimpleListener(execLatch))
                    .exec(command);
            boolean wait = execLatch.await(1, TimeUnit.MINUTES);
            if (wait) {
                LOGGER.info("Await for command execution was finished");
            }
            return baos.toString(StandardCharsets.UTF_8);
        } catch (InterruptedException e) {
            LOGGER.warn("Exception running command {} on pod: {}", command, e.getMessage());
        }
        return "";
    }

    public List<Pod> listPods(LabelSelector selector) {
        return client.pods().inNamespace(getNamespace()).withLabelSelector(selector).list().getItems();
    }

    public List<Pod> listPods(Map<String, String> labelSelector) {
        return client.pods().inNamespace(getNamespace()).withLabels(labelSelector).list().getItems();
    }

    public List<Pod> listPods(String key, String value) {
        return listPods(Collections.singletonMap(key, value));
    }

    public List<String> listPodNames(String key, String value) {
        return listPods(Collections.singletonMap(key, value)).stream()
                .map(pod -> pod.getMetadata().getName())
                .collect(Collectors.toList());
    }


    public List<PersistentVolumeClaim> listPersistentVolumeClaims() {
        return client.persistentVolumeClaims().inNamespace(getNamespace()).list().getItems();
    }

    public List<Pod> listPods() {
        return client.pods().inNamespace(getNamespace()).list().getItems();
    }

    /**
     * Returns list of pods by prefix in pod name
     *
     * @param podNamePrefix prefix with which the name should begin
     * @return List of pods
     */
    public List<Pod> listPodsByPrefixInName(String podNamePrefix) {
        return listPods()
                .stream().filter(p -> p.getMetadata().getName().startsWith(podNamePrefix))
                .collect(Collectors.toList());
    }

    /**
     * Gets pod
     */
    public Pod getPod(String name) {
        return client.pods().inNamespace(getNamespace()).withName(name).get();
    }

    /**
     * Gets pod Uid
     */
    public String getPodUid(String name) {
        return client.pods().inNamespace(getNamespace()).withName(name).get().getMetadata().getUid();
    }

    /**
     * Deletes pod
     */
    public Boolean deletePod(Pod pod) {
        return client.pods().inNamespace(getNamespace()).delete(pod);
    }

    /**
     * Deletes pod
     */
    public Boolean deletePod(LabelSelector labelSelector) {
        return client.pods().inNamespace(getNamespace()).withLabelSelector(labelSelector).delete();
    }

    public Date getCreationTimestampForPod(String podName) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'kk:mm:ss'Z'");
        Pod pod = getPod(podName);
        Date parsedDate = null;
        try {
            parsedDate = df.parse(pod.getMetadata().getCreationTimestamp());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return parsedDate;
    }

    /**
     * Gets stateful set
     */
    public StatefulSet getStatefulSet(String statefulSetName) {
        return client.apps().statefulSets().inNamespace(getNamespace()).withName(statefulSetName).get();
    }

    /**
     * Gets stateful set selectors
     */
    public LabelSelector getStatefulSetSelectors(String statefulSetName) {
        return client.apps().statefulSets().inNamespace(getNamespace()).withName(statefulSetName).get().getSpec().getSelector();
    }

    /**
     * Gets stateful set status
     */
    public boolean getStatefulSetStatus(String statefulSetName) {
        return client.apps().statefulSets().inNamespace(getNamespace()).withName(statefulSetName).isReady();
    }

    /**
     * Gets stateful set Uid
     */
    public String getStatefulSetUid(String statefulSetName) {
        return getStatefulSet(statefulSetName).getMetadata().getUid();
    }

    public void deleteStatefulSet(String statefulSetName) {
        client.apps().statefulSets().inNamespace(getNamespace()).withName(statefulSetName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    public Deployment createOrReplaceDeployment(Deployment deployment) {
        return client.apps().deployments().inNamespace(getNamespace()).createOrReplace(deployment);
    }

    /**
     * Gets deployment
     */
    public Deployment getDeployment(String deploymentName) {
        return client.apps().deployments().inNamespace(getNamespace()).withName(deploymentName).get();
    }

    public String getDeploymentNameByPrefix(String namePrefix) {
        return client.apps().deployments().inNamespace(getNamespace()).list().getItems().stream()
                .filter(rs -> rs.getMetadata().getName().startsWith(namePrefix)).collect(Collectors.toList()).get(0).getMetadata().getName();
    }

    /**
     * Gets deployment UID
     */
    public String getDeploymentUid(String deploymentName) {
        return getDeployment(deploymentName).getMetadata().getUid();
    }

    /**
     * Gets deployment status
     */
    public LabelSelector getDeploymentSelectors(String deploymentName) {
        return client.apps().deployments().inNamespace(getNamespace()).withName(deploymentName).get().getSpec().getSelector();
    }

    /**
     * Gets deployment config
     */
    public DeploymentConfig getDeploymentConfig(String deploymentConfigName) {
        return client.adapt(OpenShiftClient.class).deploymentConfigs().inNamespace(getNamespace()).withName(deploymentConfigName).get();
    }

    /**
     * Gets deployment config selector
     */
    public Map<String, String> getDeploymentConfigSelectors(String deploymentConfigName) {
        return client.adapt(OpenShiftClient.class).deploymentConfigs().inNamespace(getNamespace()).withName(deploymentConfigName).get().getSpec().getSelector();
    }

    /**
     * Delete deployment config
     *
     * @param deploymentConfigName deployment config name
     */
    public void deleteDeploymentConfig(String deploymentConfigName) {
        client.adapt(OpenShiftClient.class).deploymentConfigs().inNamespace(getNamespace()).withName(deploymentConfigName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    /**
     * Gets deployment status
     */
    public boolean getDeploymentStatus(String deploymentName) {
        return client.apps().deployments().inNamespace(getNamespace()).withName(deploymentName).isReady();
    }

    public void deleteDeployment(String deploymentName) {
        client.apps().deployments().inNamespace(getNamespace()).withName(deploymentName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    public String getReplicaSetNameByPrefix(String namePrefix) {
        return client.apps().replicaSets().inNamespace(getNamespace()).list().getItems().stream()
                .filter(rs -> rs.getMetadata().getName().startsWith(namePrefix)).collect(Collectors.toList()).get(0).getMetadata().getName();
    }

    public boolean replicaSetExists(String replicaSetName) {
        return client.apps().replicaSets().inNamespace(getNamespace()).list().getItems().stream().anyMatch(rs -> rs.getMetadata().getName().startsWith(replicaSetName));
    }

    public void deleteReplicaSet(String replicaSetName) {
        client.apps().replicaSets().inNamespace(getNamespace()).withName(replicaSetName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    /**
     * Gets deployment config status
     */
    public boolean getDeploymentConfigStatus(String deploymentConfigName) {
        return client.adapt(OpenShiftClient.class).deploymentConfigs().inNamespace(getNamespace()).withName(deploymentConfigName).isReady();
    }

    public Secret createSecret(Secret secret) {
        return client.secrets().inNamespace(getNamespace()).createOrReplace(secret);
    }

    public Secret patchSecret(String secretName, Secret secret) {
        return client.secrets().inNamespace(getNamespace()).withName(secretName).patch(secret);
    }

    public Secret getSecret(String secretName) {
        return client.secrets().inNamespace(getNamespace()).withName(secretName).get();
    }

    public boolean deleteSecret(String secretName) {
        return client.secrets().inNamespace(getNamespace()).withName(secretName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    public Service createService(Service service) {
        return client.services().inNamespace(getNamespace()).createOrReplace(service);
    }

    public Ingress createIngress(Ingress ingress) {
        return client.extensions().ingresses().inNamespace(getNamespace()).createOrReplace(ingress);
    }

    public Boolean deleteIngress(Ingress ingress) {
        return client.extensions().ingresses().inNamespace(getNamespace()).delete(ingress);
    }

    public List<Node> listNodes() {
        return client.nodes().list().getItems();
    }

    public List<Secret> listSecrets() {
        return client.secrets().inNamespace(getNamespace()).list().getItems();
    }

    public List<Secret> listSecrets(String labelKey, String labelValue) {
        return listSecrets().stream()
                .filter(secret -> secret.getMetadata().getLabels() != null)
                .filter(secret -> secret.getMetadata().getLabels().containsKey(labelKey))
                .filter(secret -> secret.getMetadata().getLabels().containsValue(labelValue))
                .collect(Collectors.toList());
    }

    public Service getService(String serviceName) {
        return client.services().inNamespace(getNamespace()).withName(serviceName).get();
    }

    /**
     * Gets service uid
     *
     * @param serviceName service name
     * @return service uid
     */
    public String getServiceUid(String serviceName) {
        return getService(serviceName).getMetadata().getUid();
    }

    public boolean getServiceStatus(String serviceName) {
        return client.services().inNamespace(getNamespace()).withName(serviceName).isReady();
    }

    public List<Service> listServices() {
        return client.services().inNamespace(getNamespace()).list().getItems();
    }

    public void deleteService(String serviceName) {
        client.services().inNamespace(getNamespace()).withName(serviceName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    public void deleteService(Service service) {
        client.services().inNamespace(getNamespace()).delete(service);
    }

    public List<ServiceAccount> listServiceAccounts() {
        return client.serviceAccounts().inNamespace(getNamespace()).list().getItems();
    }


    public String logs(String podName) {
        return client.pods().inNamespace(getNamespace()).withName(podName).getLog();
    }

    public String logs(String podName, String containerName) {
        return client.pods().inNamespace(getNamespace()).withName(podName).inContainer(containerName).getLog();
    }

    public String terminatedLogs(String podName, String containerName) {
        return client.pods().inNamespace(getNamespace()).withName(podName).inContainer(containerName).terminated().getLog();
    }

    @SuppressWarnings("deprecation")
    public List<Event> listEvents() {
        return client.events().inNamespace(getNamespace()).list().getItems();
    }

    @SuppressWarnings("deprecation")
    public List<Event> listEvents(String resourceType, String resourceName) {
        return client.events().inNamespace(getNamespace()).list().getItems().stream()
                .filter(event -> event.getInvolvedObject().getKind().equals(resourceType))
                .filter(event -> event.getInvolvedObject().getName().equals(resourceName))
                .collect(Collectors.toList());
    }

    public List<Event> listEvents(String resourceUid) {
        return listEvents().stream()
                .filter(event -> event.getInvolvedObject().getUid().equals(resourceUid))
                .collect(Collectors.toList());
    }

    public RoleBinding createOrReplaceRoleBinding(RoleBinding roleBinding) {
        return client.rbac().roleBindings().inNamespace(getNamespace()).createOrReplace(roleBinding);
    }

    public ClusterRoleBinding createOrReplaceClusterRoleBinding(ClusterRoleBinding clusterRoleBinding) {
        return client.rbac().clusterRoleBindings().inNamespace(getNamespace()).createOrReplace(clusterRoleBinding);
    }

    public Boolean deleteClusterRoleBinding(ClusterRoleBinding clusterRoleBinding) {
        return client.rbac().clusterRoleBindings().inNamespace(getNamespace()).delete(clusterRoleBinding);
    }

    public List<RoleBinding> listRoleBindings() {
        return client.rbac().roleBindings().list().getItems();
    }

    public void createOrReplaceService(String namespace, Service service) {
        client.services().inNamespace(namespace).createOrReplace(service);
    }

    public void createOrReplaceDeploymentConfig(String namespace, DeploymentConfig deploymentConfig) {
        OpenShiftClient openShiftClient = client.adapt(OpenShiftClient.class);
        openShiftClient.deploymentConfigs().inNamespace(namespace).createOrReplace(deploymentConfig);
    }

    public void createOrReplaceRoute(String namespace, Route route) {
        OpenShiftClient openShiftClient = client.adapt(OpenShiftClient.class);
        openShiftClient.routes().inNamespace(namespace).createOrReplace(route);
    }

    public List<Container> getContainersFromPod(String podName) {
        Objects.requireNonNull(podName);
        return client.pods().inNamespace(namespace).withName(podName).get().getSpec().getContainers();
    }

    private static class SimpleListener implements ExecListener {
        CountDownLatch execLatch;

        SimpleListener(CountDownLatch execLatch) {
            this.execLatch = execLatch;
            execLatch.countDown();
        }

        @Override
        public void onOpen(Response response) {
            LOGGER.info("The shell will remain open for 10 seconds.");
            execLatch.countDown();
        }

        @Override
        public void onFailure(Throwable t, Response response) {
            LOGGER.info("shell barfed with code {} and message {}", response.code(), response.message());
            execLatch.countDown();
        }

        @Override
        public void onClose(int code, String reason) {
            LOGGER.info("The shell will now close with error code {} by reason {}", code, reason);
            execLatch.countDown();
        }
    }

    /**
     * Method which return kubernetes version
     *
     * @return kubernetes version
     */
    public String clusterKubernetesVersion() {
        // This is basically workaround cause this.client.getVersion() returns null every time
        VersionInfo versionInfo = new DefaultKubernetesClient().getVersion();
        return versionInfo.getMajor() + "." + versionInfo.getMinor().replace("+", "");
    }

    /**
     * Method which return list of kube cluster nodes
     *
     * @return list of nodes
     */
    public List<Node> getClusterNodes() {
        return client.nodes().list().getItems();
    }

    /**
     * Method which return list of kube cluster workers node
     *
     * @return list of worker nodes
     */
    public List<Node> getClusterWorkers() {
        return getClusterNodes().stream().filter(node ->
                node.getMetadata().getLabels().containsKey("node-role.kubernetes.io/worker")).collect(Collectors.toList());
    }
}
