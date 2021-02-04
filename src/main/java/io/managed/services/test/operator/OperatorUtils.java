package io.managed.services.test.operator;

import com.openshift.cloud.v1alpha.models.ManagedKafkaConnection;
import com.openshift.cloud.v1alpha.models.ManagedKafkaConnectionList;
import com.openshift.cloud.v1alpha.models.ManagedKafkaRequest;
import com.openshift.cloud.v1alpha.models.ManagedKafkaRequestList;
import com.openshift.cloud.v1alpha.models.ManagedServiceAccountRequest;
import com.openshift.cloud.v1alpha.models.ManagedServiceAccountRequestList;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

import java.util.Map;

public class OperatorUtils {

    public static MixedOperation<ManagedServiceAccountRequest, ManagedServiceAccountRequestList, Resource<ManagedServiceAccountRequest>> managedServiceAccount(KubernetesClient client) {
        return client.customResources(ManagedServiceAccountRequest.class, ManagedServiceAccountRequestList.class);
    }

    public static MixedOperation<ManagedKafkaRequest, ManagedKafkaRequestList, Resource<ManagedKafkaRequest>> managedKafka(KubernetesClient client) {
        return client.customResources(ManagedKafkaRequest.class, ManagedKafkaRequestList.class);
    }

    public static MixedOperation<ManagedKafkaConnection, ManagedKafkaConnectionList, Resource<ManagedKafkaConnection>> managedKafkaConnection(KubernetesClient client) {
        return client.customResources(ManagedKafkaConnection.class, ManagedKafkaConnectionList.class);
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
