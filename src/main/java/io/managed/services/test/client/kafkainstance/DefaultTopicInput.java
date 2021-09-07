package io.managed.services.test.client.kafkainstance;

import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.TopicSettings;

public class DefaultTopicInput {

    private final String name;

    private int numPartitions = 1;

    public DefaultTopicInput(String name) {
        this.name = name;
    }

    public DefaultTopicInput numPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
        return this;
    }

    public NewTopicInput build() {
        return new NewTopicInput()
            .name(this.name)
            .settings(new TopicSettings()
                .numPartitions(this.numPartitions));
    }
}
