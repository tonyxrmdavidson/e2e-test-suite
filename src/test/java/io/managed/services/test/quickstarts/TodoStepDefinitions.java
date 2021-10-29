package io.managed.services.test.quickstarts;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.testng.SkipException;

public class TodoStepDefinitions {

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


    @Given("You’ve created a Kafka instance in OpenShift Streams for Apache Kafka")
    public void you_ve_created_a_kafka_instance_in_open_shift_streams_for_apache_kafka() {
        // Write code here that turns the phrase above into concrete actions
        throw new SkipException("TODO");
    }

    @Given("The RHOAS Operator is installed on your OpenShift cluster")
    public void the_rhoas_operator_is_installed_on_your_open_shift_cluster() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("you're logged in to the OpenShift CLI as a user who has privileges to create a new project")
    public void you_re_logged_in_to_the_open_shift_cli_as_a_user_who_has_privileges_to_create_a_new_project() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you create a new project")
    public void you_create_a_new_project() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you deploy the `quay.io\\/rhosak\\/rhoas-tools` image provided by Streams for Apache Kafka")
    public void you_deploy_the_quay_io_rhosak_rhoas_tools_image_provided_by_streams_for_apache_kafka() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you enter the `rhoas` command in the pod for the tools application")
    public void you_enter_the_rhoas_command_in_the_pod_for_the_tools_application() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("the command line shows help text for the RHOAS CLI")
    public void the_command_line_shows_help_text_for_the_rhoas_cli() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you log in to the RHOAS CLI")
    public void you_log_in_to_the_rhoas_cli() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you enter the `rhoas cluster status` command in the pod for the tools application")
    public void you_enter_the_rhoas_cluster_status_command_in_the_pod_for_the_tools_application() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("the command line shows that the RHOAS Operator was successfully installed and displays the name of the current OpenShift project")
    public void the_command_line_shows_that_the_rhoas_operator_was_successfully_installed_and_displays_the_name_of_the_current_open_shift_project() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("you copied an API token from `https:\\/\\/console.redhat.com\\/openshift\\/token`")
    public void you_copied_an_api_token_from_https_console_redhat_com_openshift_token() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you enter the `rhoas cluster connect` command in the pod for the tools application")
    public void you_enter_the_rhoas_cluster_connect_command_in_the_pod_for_the_tools_application() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you specify the Kafka instance that you want to connect to OpenShift")
    public void you_specify_the_kafka_instance_that_you_want_to_connect_to_open_shift() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you type `y` and press *Enter*")
    public void you_type_y_and_press_enter() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you paste your API token on the command line and press *Enter*")
    public void you_paste_your_api_token_on_the_command_line_and_press_enter() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("the command line shows `KafkaConnection successfully installed on your cluster`")
    public void the_command_line_shows_kafka_connection_successfully_installed_on_your_cluster() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you enter the `oc get KafkaConnection` command in the pod for the tools application")
    public void you_enter_the_oc_get_kafka_connection_command_in_the_pod_for_the_tools_application() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("the command line shows the name of a `KafkaConnection` object that corresponds to the name of your Kafka instance")
    public void the_command_line_shows_the_name_of_a_kafka_connection_object_that_corresponds_to_the_name_of_your_kafka_instance() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }


    @Given("You have a running Kafka instance in OpenShift Streams for Apache Kafka")
    public void you_have_a_running_kafka_instance_in_open_shift_streams_for_apache_kafka() {
        // Write code here that turns the phrase above into concrete actions
        throw new SkipException("TODO");
    }

    @Given("Git is installed")
    public void git_is_installed() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("You have an IDE such as IntelliJ IDEA, Eclipse, or VSCode")
    public void you_have_an_ide_such_as_intelli_j_idea_eclipse_or_vs_code() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("JDK {int} or later is installed")
    public void jdk_or_later_is_installed(Integer int1) {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("Apache Maven {double}.{int} or later is installed")
    public void apache_maven_or_later_is_installed(Double double1, Integer int1) {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("For Windows, the latest version of Oracle JDK is installed")
    public void for_windows_the_latest_version_of_oracle_jdk_is_installed() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you clone the `app-services-guides` repository from GitHub")
    public void you_clone_the_app_services_guides_repository_from_git_hub() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("the `app-services-guides` repository is available locally")
    public void the_app_services_guides_repository_is_available_locally() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("you have the bootstrap server endpoint, the service account credentials, and the SASL\\/OAUTHBEARER token endpoint for the Kafka instance")
    public void you_have_the_bootstrap_server_endpoint_the_service_account_credentials_and_the_sasl_oauthbearer_token_endpoint_for_the_kafka_instance() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you set the Kafka instance bootstrap server endpoint, service account credentials, and SASL\\/OAUTHBEARER token endpoint as environment variables")
    public void you_set_the_kafka_instance_bootstrap_server_endpoint_service_account_credentials_and_sasl_oauthbearer_token_endpoint_as_environment_variables() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("the `src\\/main\\/resources\\/application.properties` file of the Quarkus example application contains all required configurations to authenticate the Kafka instance")
    public void the_src_main_resources_application_properties_file_of_the_quarkus_example_application_contains_all_required_configurations_to_authenticate_the_kafka_instance() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you create a Kafka topic called `prices`")
    public void you_create_a_kafka_topic_called_prices() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("the `prices` Kafka topic is listed in the topics table")
    public void the_prices_kafka_topic_is_listed_in_the_topics_table() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("you've configured the Quarkus example application to connect to the Kafka instance")
    public void you_ve_configured_the_quarkus_example_application_to_connect_to_the_kafka_instance() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you navigate to the `code-examples\\/quarkus-kafka-quickstart` of the Quarkus example application and run the applications")
    public void you_navigate_to_the_code_examples_quarkus_kafka_quickstart_of_the_quarkus_example_application_and_run_the_applications() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("the application is running and the `Last price` is updated at http:\\/\\/localhost:{int}\\/prices.html")
    public void the_application_is_running_and_the_last_price_is_updated_at_http_localhost_prices_html(Integer int1) {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }
}
