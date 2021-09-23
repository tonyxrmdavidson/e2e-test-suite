Feature: Quick starts for OpenShift Streams for Apache Kafka

  A quick start is a guided tutorial with user tasks. OpenShift Streams for Apache Kafka provides several quick starts that are automatically installed in the quick start catalog of the OpenShift web console when you install the RHOAS Operator. These quick starts help you get started quickly with the main features and functionality of Streams for Apache Kafka and with connecting OpenShift-based applications to the service.

  Scenario: Connecting OpenShift to Red Hat OpenShift Streams for Apache Kafka using the CLI
    Given you have a Red Hat account
    * You’ve created a Kafka instance in OpenShift Streams for Apache Kafka
    * The Kafka instance is in Ready state
    * The RHOAS Operator is installed on your OpenShift cluster

    # 1. Running the required CLI tools
    Given you're logged in to the OpenShift CLI as a user who has privileges to create a new project
    When you create a new project
    And you deploy the `quay.io/rhosak/rhoas-tools` image provided by Streams for Apache Kafka
    And you enter the `rhoas` command
    Then the command line shows help text for the RHOAS CLI

    # 2. Checking RHOAS Operator connection to your OpenShift cluster
    When you log in to the RHOAS CLI
    And you enter the `rhoas cluster status` command
    Then the command line shows that the RHOAS Operator was successfully installed and displays the name of the current OpenShift project

    # 3. Connecting a Kafka instance to your OpenShift cluster
    Given you copied an API token from `https://console.redhat.com/openshift/token`
    When you enter the `rhoas cluster connect` command
    And you specify the Kafka instance that you want to connect to OpenShift
    And you type `y` and press *Enter*
    And you paste your API token on the command line and press *Enter*
    Then the command line shows `KafkaConnection successfully installed on your cluster`
    When you enter the `oc get KafkaConnection` command
    Then the command line shows the name of a `KafkaConnection` object that corresponds to the name of your Kafka instance

  Scenario: Binding a Quarkus application in OpenShift to Red Hat OpenShift Streams for Apache Kafka using the CLI
    Given you’ve created a Kafka instance in OpenShift Streams for Apache Kafka
    * The Kafka instance is in Ready state
    * You've connected OpenShift to Red Hat OpenShift Streams for Apache Kafka using the CLI
    * You have privileges to deploy applications in the OpenShift project that you connected your Kafka instance to

    # 1. Installing the Service Binding Operator
    When you log in to OpenShift with the `dedicated-admin` or `cluster-admin` role
    And you install the Service Binding Operator
    Then the Service Binding Operator is installed in the `openshift-operators` namespace

    # 2. Deploying an example Quarkus application
    When you switch to the OpenShift project that you previously connected your Kafka instance to
    And you deploy the `quay.io/rhoas/rhoas-quarkus-kafka-quickstart` image provided by Streams for Apache Kafka
    And you open the route created for the deployed Quarkus application in a web browser
    Then a web page for the Quarkus application opens
    When you add `/prices.html` to the URL of the web page for the Quarkus application
    Then a web page entitled *Last price* opens and the `price` value appears as `N/A`

    # 3. Creating a Kafka topic for your Quarkus application
    When you create a topic called `prices` in your Kafka instance
    Then the `prices` Kafka topic is listed in the topics table

    # 4. Binding the Quarkus application to your Kafka instance
    When you log in to the RHOAS CLI
    * You enter the `rhoas cluster bind` command
    * You specify the Kafka instance that you want to bind to your OpenShift project
    * You specify the application that you want to bind your Kafka instance to
    * You type `y` and press *Enter*
    Then the Service Binding Operator binds your Kafka instance to the Quarkus application
    And the command line shows that the binding succeeded
    When you reopen the *Last price* web page in your web browser
    Then the `price` value is continuously updated
