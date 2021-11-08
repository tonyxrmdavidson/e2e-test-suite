package io.managed.services.test.quickstarts.steps;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.testng.SkipException;

public class TodoStepDefinitions {

    @Given("JDK 11 or later is installed")
    public void jdk_11_or_later_is_installed() {
        // TODO: Low priority requirement
    }

    @Given("for Windows the latest version of Oracle JDK is installed")
    public void for_windows_the_latest_version_of_oracle_jdk_is_installed() {
        // Ignore: the tests don't run on Windows
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


    @Given("Git is installed")
    public void git_is_installed() {
        // Write code here that turns the phrase above into concrete actions
        throw new SkipException("TODO");
    }

    @Given("You have an IDE such as IntelliJ IDEA, Eclipse, or VSCode")
    public void you_have_an_ide_such_as_intelli_j_idea_eclipse_or_vs_code() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("Apache Maven {double}.{int} or later is installed")
    public void apache_maven_or_later_is_installed(Double double1, Integer int1) {
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

    @Given("Node.js {int} is installed")
    public void node_js_is_installed(Integer int1) {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you clone the `reactive-example` repository from GitHub")
    public void you_clone_the_reactive_example_repository_from_git_hub() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("the `reactive-example` repository is available locally")
    public void the_reactive_example_repository_is_available_locally() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("you have the bootstrap server endpoint and the service account credentials for the Kafka instance")
    public void you_have_the_bootstrap_server_endpoint_and_the_service_account_credentials_for_the_kafka_instance() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("You created a file called `.env` at the root level of the cloned `reactive-example` repository")
    public void you_created_a_file_called_env_at_the_root_level_of_the_cloned_reactive_example_repository() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you set the Kafka instance bootstrap server endpoint and service account credentials as environment variables in the `.env` file")
    public void you_set_the_kafka_instance_bootstrap_server_endpoint_and_service_account_credentials_as_environment_variables_in_the_env_file() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you set the value of the `KAFKA_SASL_MECHANISM` environment variable to `plain` in the `.env` file")
    public void you_set_the_value_of_the_kafka_sasl_mechanism_environment_variable_to_plain_in_the_env_file() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("the `reactive-example\\/.env` file of the Node.js example application contains all configurations required to authenticate the Kafka instance")
    public void the_reactive_example_env_file_of_the_node_js_example_application_contains_all_configurations_required_to_authenticate_the_kafka_instance() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you create a Kafka topic called `countries`")
    public void you_create_a_kafka_topic_called_countries() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("you've configured the Node.js example application to connect to the Kafka instance")
    public void you_ve_configured_the_node_js_example_application_to_connect_to_the_kafka_instance() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("You've created the `countries` topic")
    public void you_ve_created_the_countries_topic() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you navigate to the `reactive-example\\/consumer-backend` directory of the cloned repository")
    public void you_navigate_to_the_reactive_example_consumer_backend_directory_of_the_cloned_repository() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you install the dependencies for the consumer component")
    public void you_install_the_dependencies_for_the_consumer_component() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you run the consumer component")
    public void you_run_the_consumer_component() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("the consumer component is running but doesn't display country names on the command line")
    public void the_consumer_component_is_running_but_doesn_t_display_country_names_on_the_command_line() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("you've opened a second command-line window or tab")
    public void you_ve_opened_a_second_command_line_window_or_tab() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you navigate to the `reactive-example\\/producer-backend` directory of the cloned repository")
    public void you_navigate_to_the_reactive_example_producer_backend_directory_of_the_cloned_repository() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you install the dependencies for the producer component")
    public void you_install_the_dependencies_for_the_producer_component() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you run the producer component")
    public void you_run_the_producer_component() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("the producer is running and displays country names")
    public void the_producer_is_running_and_displays_country_names() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("the consumer component displays the same country names as the producer on the first command line")
    public void the_consumer_component_displays_the_same_country_names_as_the_producer_on_the_first_command_line() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }


    @Given("You have a running Kafka instance in OpenShift Streams for Apache Kafka")
    public void you_have_a_running_kafka_instance_in_open_shift_streams_for_apache_kafka() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("The Kafka instance has a generated bootstrap server")
    public void the_kafka_instance_has_a_generated_bootstrap_server() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("You created a service account for the Kafka instance")
    public void you_created_a_service_account_for_the_kafka_instance() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("You have downloaded and verified the latest supported binary version of the Apache Kafka distribution")
    public void you_have_downloaded_and_verified_the_latest_supported_binary_version_of_the_apache_kafka_distribution() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("you have client ID and Secret for the service account, and a SASL connection mechanism")
    public void you_have_client_id_and_secret_for_the_service_account_and_a_sasl_connection_mechanism() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you create an `app-services.properties` file in the local `\\/config` directory of the Kafka binaries")
    public void you_create_an_app_services_properties_file_in_the_local_config_directory_of_the_kafka_binaries() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("it contains the connection values")
    public void it_contains_the_connection_values() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("you are set up to produce messages to Kafka topics")
    public void you_are_set_up_to_produce_messages_to_kafka_topics() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("The Kafka topic creation script `kafka-topics.sh` is available in the `\\/bin` directory of the Kafka binaries")
    public void the_kafka_topic_creation_script_kafka_topics_sh_is_available_in_the_bin_directory_of_the_kafka_binaries() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("The Kafka producer creation script `kafka-console-producer.sh` is available in the `\\/bin` directory of the Kafka binaries")
    public void the_kafka_producer_creation_script_kafka_console_producer_sh_is_available_in_the_bin_directory_of_the_kafka_binaries() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("An `app-services.properties` file is configured in the local `\\/config` directory of the Kafka binaries")
    public void an_app_services_properties_file_is_configured_in_the_local_config_directory_of_the_kafka_binaries() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("You have the bootstrap server address for the Kafka instance")
    public void you_have_the_bootstrap_server_address_for_the_kafka_instance() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you enter a command to create a Kafka topic using `kafka-topics.sh`")
    public void you_enter_a_command_to_create_a_kafka_topic_using_kafka_topics_sh() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("use the bootstrap server address")
    public void use_the_bootstrap_server_address() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("use the `app-services.properties` file")
    public void use_the_app_services_properties_file() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("a topic is created in the Kafka instance")
    public void a_topic_is_created_in_the_kafka_instance() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("you have created a topic")
    public void you_have_created_a_topic() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you produce messages to the topic you created using `kafka-console-producer.sh`")
    public void you_produce_messages_to_the_topic_you_created_using_kafka_console_producer_sh() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("use the bootstrap server address as a parameter")
    public void use_the_bootstrap_server_address_as_a_parameter() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("use the `app-services.properties` file as a parameter")
    public void use_the_app_services_properties_file_as_a_parameter() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("messages are produced to the topic in the Kafka instance")
    public void messages_are_produced_to_the_topic_in_the_kafka_instance() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("you are set up to consume messages")
    public void you_are_set_up_to_consume_messages() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("The Kafka consumer creation script `kafka-console-consumer.sh` is available in the `\\/bin` directory of the Kafka binaries")
    public void the_kafka_consumer_creation_script_kafka_console_consumer_sh_is_available_in_the_bin_directory_of_the_kafka_binaries() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Given("A topic contains the messages you produced in the Kafka instance")
    public void a_topic_contains_the_messages_you_produced_in_the_kafka_instance() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @When("you consume messages from the topic you created using `kafka-console-consumer.sh`")
    public void you_consume_messages_from_the_topic_you_created_using_kafka_console_consumer_sh() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("messages are consumed from the topic")
    public void messages_are_consumed_from_the_topic() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }

    @Then("displayed on the command line")
    public void displayed_on_the_command_line() {
        // Write code here that turns the phrase above into concrete actions
        throw new io.cucumber.java.PendingException();
    }
}
