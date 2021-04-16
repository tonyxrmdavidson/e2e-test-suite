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

import java.util.Map;

public class OperatorUtils {

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
}
