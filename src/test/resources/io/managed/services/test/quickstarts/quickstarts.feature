Feature: Quick starts for OpenShift Streams for Apache Kafka

  A quick start is a guided tutorial with user tasks. OpenShift Streams for Apache Kafka provides several quick starts to help you get started quickly with the main features and functionality of the service.

  Scenario: Getting started with Red Hat OpenShift Streams for Apache Kafka
    Given you have a Red Hat account

    # 1. Creating a Kafka instance in OpenShift Streams for Apache Kafka
    Given you’re logged in to the OpenShift Streams for Apache Kafka web console at https://console.redhat.com/beta/application-services/streams/
    When you create a Kafka instance with a unique name
    Then the Kafka instance is listed in the instances table
    And the Kafka instance is shown as Ready

    # 2. Creating a service account to connect to a Kafka instance in OpenShift Streams for Apache Kafka
    Given you’ve created a Kafka instance in OpenShift Streams for Apache Kafka
    * The Kafka instance is in Ready state
    * The Kafka instance has a generated bootstrap server
    When you create a service account with a unique name
    Then the service account has a generated Client ID and Client Secret
    And the service account is listed in the service accounts table

    # 3. Creating a Kafka topic in OpenShift Streams for Apache Kafka
    Given you’ve created a Kafka instance in OpenShift Streams for Apache Kafka
    * The Kafka instance is in Ready state
    When you create a Kafka topic with a unique name
    Then the Kafka topic is listed in the topics table

  Scenario: Using Quarkus applications with Kafka instances in Red Hat OpenShift Streams for Apache Kafka
    Given you have a Red Hat account
    * You have a running Kafka instance in OpenShift Streams for Apache Kafka
    * Git is installed
    * You have an IDE such as IntelliJ IDEA, Eclipse, or VSCode
    * JDK 11 or later is installed
    * Apache Maven 3.6.2 or later is installed
    * For Windows, the latest version of Oracle JDK is installed

    # 1. Importing the Quarkus sample code
    When you clone the `app-services-guides` repository from GitHub
    Then the `app-services-guides` repository is available locally

    # 2. Configuring the Quarkus example application to connect to a Kafka instance
    Given you have the bootstrap server endpoint, the service account credentials, and the SASL/OAUTHBEARER token endpoint for the Kafka instance
    When you set the Kafka instance bootstrap server endpoint, service account credentials, and SASL/OAUTHBEARER token endpoint as environment variables
    Then the `src/main/resources/application.properties` file of the Quarkus example application contains all required configurations to authenticate the Kafka instance

    # 3. Creating the prices Kafka topic in OpenShift Streams for Apache Kafka
    Given you’ve created a Kafka instance in OpenShift Streams for Apache Kafka
    * The Kafka instance is in Ready state
    When you create a Kafka topic called `prices`
    Then the `prices` Kafka topic is listed in the topics table

    #4. Running the Quarkus example application
    Given you've configured the Quarkus example application to connect to the Kafka instance
    * You’ve created a Kafka instance in OpenShift Streams for Apache Kafka
    * The Kafka instance is in Ready state
    When you navigate to the `code-examples/quarkus-kafka-quickstart` of the Quarkus example application and run the applications
    Then the application is running and the `Last price` is updated at http://localhost:8080/prices.html

  Scenario: Using Kafkacat with Kafka instances in Red Hat OpenShift Streams for Apache Kafka
    Given you have a Red Hat account
    * You have a running Kafka instance in OpenShift Streams for Apache Kafka
    * JDK 11 or later is installed
    * For Windows, the latest version of Oracle JDK is installed

    # 1. Installing and verifying Kafkacat
    When you install Kafkacat and check the version on the command line
    Then the expected Kafkacat version is installed and displayed

    # 2. Configuring Kafkacat to connect to a Kafka instance
    Given you have the bootstrap server endpoint and the service account credentials for the Kafka instance
    When you set the Kafka instance bootstrap server endpoint and service account credentials as environment variables
    Then Kafkacat can access all required configurations to authenticate the Kafka instance

    # 3. Producing messages in Kafkacat
    Given Kafkacat is installed
    * You have a running Kafka instance in OpenShift Streams for Apache Kafka
    * The Kafka instance is in Ready state
    * You’ve set the Kafka bootstrap server endpoint and your service account credentials as environment variables
    When you start Kafkacat in producer mode
    And you enter messages into Kafkacat that you want to produce
    Then messages are produced to the specified topic in the configured Kafka instance

    #4. Consuming messages in Kafkacat
    Given Kafkacat is installed
    * You have a running Kafka instance in OpenShift Streams for Apache Kafka
    * The instance is in Ready state
    * You’ve set the Kafka bootstrap server endpoint and your service account credentials as environment variables
    * You used a producer to produce example messages to a topic
    When you start Kafkacat in consumer mode
    Then messages from the producer are consumed and displayed on the command line
