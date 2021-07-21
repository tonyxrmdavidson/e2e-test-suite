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
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.managed.services.test.Environment;
import io.managed.services.test.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public class OperatorUtils {
    private static final Logger LOGGER = LogManager.getLogger(OperatorUtils.class);

    public static MixedOperation<CloudServiceAccountRequest, CloudServiceAccountRequestList, Resource<CloudServiceAccountRequest>> cloudServiceAccountRequest(KubernetesClient client) {
        return client.customResources(CloudServiceAccountRequest.class, CloudServiceAccountRequestList.class);
    }

    public static MixedOperation<CloudServicesRequest, CloudServicesRequestList, Resource<CloudServicesRequest>> cloudServicesRequest(KubernetesClient client) {
        return client.customResources(CloudServicesRequest.class, CloudServicesRequestList.class);
    }

    public static MixedOperation<KafkaConnection, KafkaConnectionList, Resource<KafkaConnection>> kafkaConnection(KubernetesClient client) {
        return client.customResources(KafkaConnection.class, KafkaConnectionList.class);
    }

    public static MixedOperation<ServiceBinding, ServiceBindingList, Resource<ServiceBinding>> serviceBinding(KubernetesClient client) {
        return client.customResources(ServiceBinding.class, ServiceBindingList.class);
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
    public static void patchTheOperatorCloudServiceAPIEnv(KubernetesClient client) throws IOException {

        final var clusterServiceVersionsContext = new CustomResourceDefinitionContext.Builder()
            .withName("clusterserviceversions.operators.coreos.com")
            .withGroup("operators.coreos.com")
            .withVersion("v1alpha1")
            .withPlural("clusterserviceversions")
            .withScope("Namespaced")
            .build();

        LOGGER.info("find rhoas-operator ClusterServiceVersion");
        final var csvs = client.customResource(clusterServiceVersionsContext)
            .list(Environment.RHOAS_OPERATOR_NAMESPACE);

        final var csvName = new JSONObject(csvs)
            .getArray("items")
            .findObject(o -> o.getObject("metadata").getString("name").startsWith("rhoas-operator."))
            .getObject("metadata")
            .getString("name");

        LOGGER.info("patch {} ClusterServiceVersion", csvName);
        var csv = client.customResource(clusterServiceVersionsContext)
            .get(Environment.RHOAS_OPERATOR_NAMESPACE, csvName);

        // edit the CLOUD_SERVICES_API env
        new JSONObject(csv)
            .getObject("spec")
            .getObject("install")
            .getObject("spec")
            .getArray("deployments")
            .findObject("name", "rhoas-operator")
            .getObject("spec")
            .getObject("template")
            .getObject("spec")
            .getArray("containers")
            .findObject("name", "rhoas-operator")
            .getArray("env")
            .findObject("name", "CLOUD_SERVICES_API")
            .put("value", Environment.SERVICE_API_URI);

        client.customResource(clusterServiceVersionsContext)
            .edit(Environment.RHOAS_OPERATOR_NAMESPACE, csvName, csv);
    }
}
