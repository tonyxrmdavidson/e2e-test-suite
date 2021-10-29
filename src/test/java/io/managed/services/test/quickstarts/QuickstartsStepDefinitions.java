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
import org.testng.SkipException;

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
            .region(Environment.DEFAULT_KAFKA_REGION);

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

    @Given("the Kafka instance has a generated Bootstrap server")
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

    @Given("you have a Red Hat account")
    public void you_have_a_red_hat_account() {
        // Write code here that turns the phrase above into concrete actions
        throw new SkipException("TODO");
    }

    @Given("you’re logged in to the OpenShift Streams for Apache Kafka web console")
    public void you_re_logged_in_to_the_open_shift_streams_for_apache_kafka_web_console() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you create a Kafka instance with a unique name")
    public void you_create_a_kafka_instance_with_a_unique_name() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("you’ve created a Kafka instance in OpenShift Streams for Apache Kafka")
    public void you_ve_created_a_kafka_instance_in_open_shift_streams_for_apache_kafka() {
        // Write code here that turns the phrase above into concrete actions
        throw new SkipException("TODO");
    }

    @Given("The Kafka instance is in Ready state")
    public void the_kafka_instance_is_in_ready_state() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you create a service account with a unique name")
    public void you_create_a_service_account_with_a_unique_name() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you create a Kafka topic with a unique name")
    public void you_create_a_kafka_topic_with_a_unique_name() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("your OpenShift cluster is running on OpenShift {double} or later")
    public void your_open_shift_cluster_is_running_on_open_shift_or_later(Double double1) {
        // Write code here that turns the phrase above into concrete actions
        throw new SkipException("TODO");
    }

    @Given("You've connected OpenShift to Red Hat OpenShift Streams for Apache Kafka using the CLI")
    public void you_ve_connected_open_shift_to_red_hat_open_shift_streams_for_apache_kafka_using_the_cli() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("You have privileges to deploy applications in the OpenShift project that you connected your Kafka instance to")
    public void you_have_privileges_to_deploy_applications_in_the_open_shift_project_that_you_connected_your_kafka_instance_to() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("You've installed Git")
    public void you_ve_installed_git() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you log in to OpenShift with the `dedicated-admin` or `cluster-admin` role")
    public void you_log_in_to_open_shift_with_the_dedicated_admin_or_cluster_admin_role() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you install the Service Binding Operator")
    public void you_install_the_service_binding_operator() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("the Service Binding Operator is installed in the `openshift-operators` namespace")
    public void the_service_binding_operator_is_installed_in_the_openshift_operators_namespace() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you switch to the OpenShift project that you previously connected your Kafka instance to")
    public void you_switch_to_the_open_shift_project_that_you_previously_connected_your_kafka_instance_to() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you clone the `nodeshift-starters\\/reactive-example` repository from GitHub")
    public void you_clone_the_nodeshift_starters_reactive_example_repository_from_git_hub() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you navigate to the `consumer-backend` directory of the cloned repository")
    public void you_navigate_to_the_consumer_backend_directory_of_the_cloned_repository() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you enter the `npm install` command on the command line")
    public void you_enter_the_npm_install_command_on_the_command_line() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("Node Package Manager installs dependencies for the consumer component of the Node.js application")
    public void node_package_manager_installs_dependencies_for_the_consumer_component_of_the_node_js_application() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you enter the `npm run openshift` command")
    public void you_enter_the_npm_run_openshift_command() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("Node Package Manager builds the consumer component and deploys it to your OpenShift project")
    public void node_package_manager_builds_the_consumer_component_and_deploys_it_to_your_open_shift_project() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("the logs of the pod for the deployed consumer component show that the component can't connect to Kafka")
    public void the_logs_of_the_pod_for_the_deployed_consumer_component_show_that_the_component_can_t_connect_to_kafka() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you navigate to the `producer-backend` directory of the cloned repository")
    public void you_navigate_to_the_producer_backend_directory_of_the_cloned_repository() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("Node Package Manager installs dependencies for the producer component of the Node.js application")
    public void node_package_manager_installs_dependencies_for_the_producer_component_of_the_node_js_application() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("Node Package Manager builds the producer component and deploys it to your OpenShift project")
    public void node_package_manager_builds_the_producer_component_and_deploys_it_to_your_open_shift_project() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("the logs of the pod for the deployed producer component show that the component can't connect to Kafka")
    public void the_logs_of_the_pod_for_the_deployed_producer_component_show_that_the_component_can_t_connect_to_kafka() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you create a topic called `countries` in your Kafka instance")
    public void you_create_a_topic_called_countries_in_your_kafka_instance() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("the `countries` Kafka topic is listed in the topics table")
    public void the_countries_kafka_topic_is_listed_in_the_topics_table() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you log in to the OpenShift web console as the same user who deployed the Node.js application")
    public void you_log_in_to_the_open_shift_web_console_as_the_same_user_who_deployed_the_node_js_application() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("You switch to the *Developer* perspective")
    public void you_switch_to_the_developer_perspective() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("You open the *Topology* page of the console")
    public void you_open_the_topology_page_of_the_console() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("You left-click and drag a binding connection from the icon for the producer component to the icon for the `KafkaConnection` object")
    public void you_left_click_and_drag_a_binding_connection_from_the_icon_for_the_producer_component_to_the_icon_for_the_kafka_connection_object() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("You left-click and drag a binding connection from the icon for the consumer component to the icon for the `KafkaConnection` object")
    public void you_left_click_and_drag_a_binding_connection_from_the_icon_for_the_consumer_component_to_the_icon_for_the_kafka_connection_object() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("the Service Binding Operator binds your Kafka instance to each component")
    public void the_service_binding_operator_binds_your_kafka_instance_to_each_component() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("the logs of the pod for the producer component show that the producer generates random country names")
    public void the_logs_of_the_pod_for_the_producer_component_show_that_the_producer_generates_random_country_names() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("the logs of the pod for the consumer component show that the consumer consumes the same country names, and in the same order")
    public void the_logs_of_the_pod_for_the_consumer_component_show_that_the_consumer_consumes_the_same_country_names_and_in_the_same_order() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }
}
