package io.managed.services.test.quickstarts;

import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.TopicSettings;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.KafkaRequestPayload;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import com.openshift.cloud.api.kas.models.ServiceAccountRequest;
import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.managed.services.test.Environment;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
import io.managed.services.test.kafka.KafkaMgmtAPITest;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

import static io.managed.services.test.TestUtils.bwait;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class QuickstartsStepDefinitions {
    private static final Logger LOGGER = LogManager.getLogger(KafkaMgmtAPITest.class);

    private static final String KAFKA_INSTANCE_NAME = "cucumber-qs-" + Environment.LAUNCH_KEY;
    private static final String SERVICE_ACCOUNT_NAME = "cucumber-sa-qs-" + Environment.LAUNCH_KEY;
    private static final String TOPIC_NAME = "test-topic";

    private KafkaMgmtApi kafkaMgmtApi;
    private SecurityMgmtApi securityMgmtApi;
    private KafkaInstanceApi kafkaInstanceApi;

    private KafkaRequest kafkaInstance;
    private ServiceAccount serviceAccount;

    @Given("I am authenticated to Red Hat SSO")
    public void i_am_authenticated_to_red_hat_sso() {
        var apis = ApplicationServicesApi.applicationServicesApi(
            Environment.PRIMARY_USERNAME,
            Environment.PRIMARY_PASSWORD);

        kafkaMgmtApi = apis.kafkaMgmt();
        securityMgmtApi = apis.securityMgmt();
    }

    @When("I create a Kafka instance with a unique name")
    @SneakyThrows
    public void i_create_a_kafka_instance_with_a_unique_name() {

        var payload = new KafkaRequestPayload()
            .name(KAFKA_INSTANCE_NAME)
            .multiAz(true)
            .cloudProvider("aws")
            .region("us-east-1");

        kafkaInstance = KafkaMgmtApiUtils.createKafkaInstance(kafkaMgmtApi, payload);
        LOGGER.debug(kafkaInstance);
    }

    @Then("the Kafka instance is listed in the instances table")
    @SneakyThrows
    public void the_kafka_instance_is_listed_in_the_instances_table() {
        var list = kafkaMgmtApi.getKafkas(null, null, null, null);
        LOGGER.debug(list);

        var o = list.getItems().stream()
            .filter(k -> KAFKA_INSTANCE_NAME.equals(k.getName()))
            .findAny();
        assertTrue(o.isPresent());
    }

    @Then("the Kafka instance is shown as Ready")
    public void the_kafka_instance_is_shown_as_ready() throws Throwable {
        kafkaInstance = KafkaMgmtApiUtils.waitUntilKafkaIsReady(kafkaMgmtApi, kafkaInstance.getId());
        LOGGER.debug(kafkaInstance);

        assertEquals(kafkaInstance.getStatus(), "ready");
    }

    @Then("the Kafka instance has a generated Bootstrap server")
    public void the_kafka_instance_has_a_generated_bootstrap_server() {
        assertNotNull(kafkaInstance.getBootstrapServerHost());
    }

    @When("I create a service account with a unique name")
    public void i_create_a_service_account_with_a_unique_name() throws Throwable {
        // Create Service Account
        var payload = new ServiceAccountRequest().name(SERVICE_ACCOUNT_NAME);
        serviceAccount = securityMgmtApi.createServiceAccount(payload);
        LOGGER.debug(serviceAccount);
    }

    @Then("the service account has a generated Client ID and Client Secret")
    public void the_service_account_has_a_generated_client_id_and_client_secret() {
        assertNotNull(serviceAccount.getClientId());
        assertNotNull(serviceAccount.getClientSecret());
    }

    @Then("the service account is listed in the service accounts table")
    public void the_service_account_is_listed_in_the_service_accounts_table() throws Throwable {
        var list = securityMgmtApi.getServiceAccounts();
        LOGGER.debug(list);

        var o = list.getItems().stream()
            .filter(a -> SERVICE_ACCOUNT_NAME.equals(a.getName()))
            .findAny();
        assertTrue(o.isPresent());
    }

    @Given("a ready Kafka instance")
    public void a_ready_kafka_instance() {
        assertEquals(kafkaInstance.getStatus(), "ready");
    }

    @When("I create a Kafka topic with a unique name")
    public void i_create_a_kafka_topic_with_a_unique_name() throws Throwable {
        kafkaInstanceApi = bwait(KafkaInstanceApiUtils.kafkaInstanceApi(kafkaInstance,
            Environment.PRIMARY_USERNAME, Environment.PRIMARY_PASSWORD));

        var payload = new NewTopicInput()
            .name(TOPIC_NAME)
            .settings(new TopicSettings().numPartitions(1));
        var topic = kafkaInstanceApi.createTopic(payload);
        LOGGER.debug(topic);
    }

    @Then("the Kafka topic is listed in the topics table")
    public void the_kafka_topic_is_listed_in_the_topics_table() throws Throwable {
        var list = kafkaInstanceApi.getTopics();
        LOGGER.debug(list);

        var o = Objects.requireNonNull(list.getItems()).stream()
            .filter(t -> TOPIC_NAME.equals(t.getName()))
            .findAny();
        assertTrue(o.isPresent());
    }

    @After
    public void teardown() {
        // delete kafka instance
        try {
            KafkaMgmtApiUtils.deleteKafkaByNameIfExists(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean main kafka instance error: ", t);
        }

        // delete service account
        try {
            SecurityMgmtAPIUtils.cleanServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean service account error: ", t);
        }
    }
}
