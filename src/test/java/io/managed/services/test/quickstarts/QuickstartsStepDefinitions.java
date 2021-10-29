package io.managed.services.test.quickstarts;

import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.TopicSettings;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import com.openshift.cloud.api.kas.models.ServiceAccountRequest;
import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.managed.services.test.Environment;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.oauth.KeycloakLoginSession;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
import io.managed.services.test.kafka.KafkaMgmtAPITest;
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

    private KeycloakLoginSession keycloakLoginSession;

    private KafkaMgmtApi kafkaMgmtApi;
    private SecurityMgmtApi securityMgmtApi;
    private KafkaInstanceApi kafkaInstanceApi;

    private KafkaRequest kafkaInstance;
    private ServiceAccount serviceAccount;

    @Given("you have a Red Hat account")
    public void you_have_a_red_hat_account() {
        assertNotNull(Environment.PRIMARY_USERNAME, "the PRIMARY_USERNAME env is null");
        assertNotNull(Environment.PRIMARY_PASSWORD, "the PRIMARY_PASSWORD env is null");
    }

    @Given("you’re logged in to the OpenShift Streams for Apache Kafka web console")
    public void you_re_logged_in_to_the_open_shift_streams_for_apache_kafka_web_console() throws Throwable {

        keycloakLoginSession = new KeycloakLoginSession(Environment.PRIMARY_USERNAME, Environment.SECONDARY_PASSWORD);

        var user = bwait(keycloakLoginSession.loginToRedHatSSO());
        kafkaMgmtApi = KafkaMgmtApiUtils.kafkaMgmtApi(Environment.OPENSHIFT_API_URI, user);
        securityMgmtApi = SecurityMgmtAPIUtils.securityMgmtApi(Environment.OPENSHIFT_API_URI, user);
    }

    @When("you create a Kafka instance with a unique name")
    public void you_create_a_kafka_instance_with_a_unique_name() throws Throwable {
        var payload = KafkaMgmtApiUtils.defaultKafkaInstance(KAFKA_INSTANCE_NAME);
        kafkaInstance = KafkaMgmtApiUtils.createKafkaInstance(kafkaMgmtApi, payload);
        LOGGER.debug(kafkaInstance);
    }

    @Then("the Kafka instance is listed in the instances table")
    public void the_kafka_instance_is_listed_in_the_instances_table() throws Throwable {
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

    @Given("you’ve created a Kafka instance in OpenShift Streams for Apache Kafka")
    public void you_ve_created_a_kafka_instance_in_open_shift_streams_for_apache_kafka() throws Throwable {
        assertNotNull(kafkaInstance);
    }

    @Given("The Kafka instance is in Ready state")
    public void the_kafka_instance_is_in_ready_state() {
        assertEquals(kafkaInstance.getStatus(), "ready");
    }

    @Given("The Kafka instance has a generated bootstrap server")
    public void the_kafka_instance_has_a_generated_bootstrap_server() {
        assertNotNull(kafkaInstance.getBootstrapServerHost());
    }

    @When("you create a service account with a unique name")
    public void you_create_a_service_account_with_a_unique_name() throws Throwable {
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

    @When("you create a Kafka topic with a unique name")
    public void you_create_a_kafka_topic_with_a_unique_name() throws Throwable {
        var user = bwait(keycloakLoginSession.loginToOpenshiftIdentity());
        kafkaInstanceApi = KafkaInstanceApiUtils.kafkaInstanceApi(kafkaInstance, user);

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
