
package io.managed.services.test.operator;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResourceList;


public class ServiceBindingList extends CustomResourceList<ServiceBinding> implements Namespaced {
}
