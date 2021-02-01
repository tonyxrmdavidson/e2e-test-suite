package io.managed.services.test.utils;

import io.managed.services.test.client.serviceapi.KafkaListResponse;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;

import static io.managed.services.test.TestUtils.await;

class Utils {

    public KafkaResponse getKafkaByName(ServiceAPI api, String name) {
        KafkaListResponse response = await(api.getListOfKafkaByName(name));
        return response.items.size() == 1 ? response.items.get(0) : null;
    }
}
