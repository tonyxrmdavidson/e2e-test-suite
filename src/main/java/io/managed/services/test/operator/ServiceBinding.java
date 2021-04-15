
package io.managed.services.test.operator;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Plural;
import io.fabric8.kubernetes.model.annotation.Version;

@Plural("servicebindings")
@Group("binding.operators.coreos.com")
@Version("v1alpha1")
public class ServiceBinding extends CustomResource<ServiceBindingSpec, ServiceBindingStatus> implements Namespaced {
}
