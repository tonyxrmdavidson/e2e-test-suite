package io.managed.services.test.quickstarts.steps;

import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.TopicSettings;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.managed.services.test.quickstarts.contexts.KafkaInstanceContext;
import lombok.extern.log4j.Log4j2;

import java.util.Objects;

import static org.testng.Assert.assertTrue;

@Log4j2
public class KafkaTopicSteps {

    private static final String TOPIC_NAME = "test-topic";

    private final KafkaInstanceContext kafkaInstanceContext;

    public KafkaTopicSteps(KafkaInstanceContext kafkaInstanceContext) {
        this.kafkaInstanceContext = kafkaInstanceContext;
    }

    // TODO: Generalize topic name
    @When("you create a Kafka topic with a unique name")
    public void you_create_a_kafka_topic_with_a_unique_name() throws Throwable {
        var kafkaInstance = kafkaInstanceContext.requireKafkaInstance();
        var kafkaInstanceApi = kafkaInstanceContext.kafkaInstanceApi();

        log.info("create topic with name '{}' on kafka instance '{}'", TOPIC_NAME, kafkaInstance.getName());
        var payload = new NewTopicInput()
            .name(TOPIC_NAME)
            .settings(new TopicSettings().numPartitions(1));
        var topic = kafkaInstanceApi.createTopic(payload);
        log.debug(topic);
    }

    @Then("the Kafka topic is listed in the topics table")
    public void the_kafka_topic_is_listed_in_the_topics_table() throws Throwable {
        var kafkaInstanceApi = kafkaInstanceContext.kafkaInstanceApi();

        var list = kafkaInstanceApi.getTopics();
        log.debug(list);

        var o = Objects.requireNonNull(list.getItems()).stream()
            .filter(t -> TOPIC_NAME.equals(t.getName()))
            .findAny();
        assertTrue(o.isPresent());
    }
}
