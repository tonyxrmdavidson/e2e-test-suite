package io.managed.services.test.operator;

import com.openshift.cloud.v1alpha.models.CloudServiceAccountRequest;
import com.openshift.cloud.v1alpha.models.CloudServiceAccountRequestList;
import com.openshift.cloud.v1alpha.models.CloudServicesRequest;
import com.openshift.cloud.v1alpha.models.CloudServicesRequestList;
import com.openshift.cloud.v1alpha.models.KafkaConnection;
import com.openshift.cloud.v1alpha.models.KafkaConnectionList;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.client.OpenShiftClient;
import io.managed.services.test.Environment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
public class OperatorUtils {
    private static final Logger LOGGER = LogManager.getLogger(OperatorUtils.class);

    public static MixedOperation<CloudServiceAccountRequest, CloudServiceAccountRequestList, Resource<CloudServiceAccountRequest>> cloudServiceAccountRequest(KubernetesClient client) {
        return client.resources(CloudServiceAccountRequest.class, CloudServiceAccountRequestList.class);
    }

    public static MixedOperation<CloudServicesRequest, CloudServicesRequestList, Resource<CloudServicesRequest>> cloudServicesRequest(KubernetesClient client) {
        return client.resources(CloudServicesRequest.class, CloudServicesRequestList.class);
    }

    public static MixedOperation<KafkaConnection, KafkaConnectionList, Resource<KafkaConnection>> kafkaConnection(KubernetesClient client) {
        return client.resources(KafkaConnection.class, KafkaConnectionList.class);
    }

    public static MixedOperation<ServiceBinding, ServiceBindingList, Resource<ServiceBinding>> serviceBinding(KubernetesClient client) {
        return client.resources(ServiceBinding.class, ServiceBindingList.class);
    }

    public static Secret buildSecret(String name, Map<String, String> data) {
        return new SecretBuilder()
            .withMetadata(new ObjectMetaBuilder()
                .withName(name)
                .build())
            .withData(data)
            .build();
    }

    /**
     * Change the operator CLOUD_SERVICES_API env with Environment.SERVICE_API_URI
     */
    public static void patchTheOperatorCloudServiceAPIEnv(OpenShiftClient client) {

        LOGGER.info("find rhoas-operator ClusterServiceVersion");
        final var csv = client.operatorHub()
            .clusterServiceVersions()
            .inNamespace(Environment.RHOAS_OPERATOR_NAMESPACE)
            .list()
            .getItems()
            .stream().filter(c -> c.getMetadata().getName().startsWith("rhoas-operator.")).findAny().orElseThrow();

        LOGGER.info("patch {} ClusterServiceVersion", csv.getMetadata().getName());
        csv.getSpec()
            .getInstall()
            .getSpec()
            .getDeployments()
            .stream().filter(d -> d.getName().equals("rhoas-operator")).findAny().orElseThrow()
            .getSpec()
            .getTemplate()
            .getSpec()
            .getContainers()
            .stream().filter(c -> c.getName().equals("rhoas-operator")).findAny().orElseThrow()
            .getEnv()
            .stream().filter(c -> c.getName().equals("CLOUD_SERVICES_API")).findAny().orElseThrow()
            .setValue(Environment.SERVICE_API_URI);

        client.operatorHub()
            .clusterServiceVersions()
            .inNamespace(Environment.RHOAS_OPERATOR_NAMESPACE)
            .withName(csv.getMetadata().getName())
            .replace(csv);
    }
}
