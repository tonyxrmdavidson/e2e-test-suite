Feature: Quick starts for OpenShift Streams for Apache Kafka

  A quick start is a guided tutorial with user tasks. OpenShift Streams for Apache Kafka provides several quick starts to help you get started quickly with the main features and functionality of the service.

  Scenario: Getting started with Red Hat OpenShift Streams for Apache Kafka
    Given you have a Red Hat account

    # 1. Creating a Kafka instance in OpenShift Streams for Apache Kafka
    Given you’re logged in to the OpenShift Streams for Apache Kafka web console
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

    # 3. Setting permissions for a service account in a Kafka instance in OpenShift Streams for Apache Kafka
    Given you've created a Kafka instance
    * The Kafka instance is in Ready state
    * You've created a service account that you want to allow to interact with the running Kafka instance
    When you add permissions for the new service account in the Kafka instance
    Then the new permissions for the service account are listed in the Permissions page of the Kafka instance

    # 4. Creating a Kafka topic in OpenShift Streams for Apache Kafka
    Given you’ve created a Kafka instance in OpenShift Streams for Apache Kafka
    * The Kafka instance is in Ready state
    When you create a Kafka topic with a unique name
    Then the Kafka topic is listed in the topics table

    # 5. Viewing metrics for a Kafka instance in OpenShift Streams for Apache Kafka
    Given you’ve created a Kafka instance in OpenShift Streams for Apache Kafka
    * The Kafka instance is in Ready state
    * You've created topics in your Kafka instance
    When you open the Dashboard tab for a new Kafka instance
    Then no metrics are displayed for your new instance and topics
    When you produce and consume messages in your topics
    And you open again the Dashboard tab for the instance
    Then metrics for your Kafka instance and topics are displayed

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

  Scenario: Using Node.js applications with Kafka instances in Red Hat OpenShift Streams for Apache Kafka
    Given you have a Red Hat account
    * You have a running Kafka instance in OpenShift Streams for Apache Kafka
    * The Kafka instance is in Ready state
    * Git is installed
    * You have an IDE such as IntelliJ IDEA, Eclipse, or VSCode
    * Node.js 14 is installed

    # 1. Importing the Node.js sample code
    When you clone the `reactive-example` repository from GitHub
    Then the `reactive-example` repository is available locally

    # 2. Configuring the Node.js example application to connect to a Kafka instance
    Given you have the bootstrap server endpoint and the service account credentials for the Kafka instance
    * You created a file called `.env` at the root level of the cloned `reactive-example` repository
    When you set the Kafka instance bootstrap server endpoint and service account credentials as environment variables in the `.env` file
    And you set the value of the `KAFKA_SASL_MECHANISM` environment variable to `plain` in the `.env` file
    Then the `reactive-example/.env` file of the Node.js example application contains all configurations required to authenticate the Kafka instance

    # 3. Creating a Kafka topic in OpenShift Streams for Apache Kafka
    Given you’ve created a Kafka instance in OpenShift Streams for Apache Kafka
    * The Kafka instance is in Ready state
    When you create a Kafka topic called `countries`
    Then the `countries` Kafka topic is listed in the topics table

    #4. Running the Node.js example application
    Given you've configured the Node.js example application to connect to the Kafka instance
    * You’ve created a Kafka instance in OpenShift Streams for Apache Kafka
    * The Kafka instance is in Ready state
    * You've created the `countries` topic
    When you navigate to the `reactive-example/consumer-backend` directory of the cloned repository
    And you install the dependencies for the consumer component
    And you run the consumer component
    Then the consumer component is running but doesn't display country names on the command line
    Given you've opened a second command-line window or tab
    When you navigate to the `reactive-example/producer-backend` directory of the cloned repository
    And you install the dependencies for the producer component
    And you run the producer component
    Then the producer is running and displays country names
    And the consumer component displays the same country names as the producer on the first command line

  Scenario: Using Kcat with Kafka instances in Red Hat OpenShift Streams for Apache Kafka
    Given you have a Red Hat account
    * You have a running Kafka instance in OpenShift Streams for Apache Kafka
    * JDK 11 or later is installed
    * For Windows, the latest version of Oracle JDK is installed
    * You have downloaded and verified the latest supported version of Kcat for your operating system

    # 1. Configuring Kcat to connect to a Kafka instance
    Given you have the bootstrap server endpoint and the service account credentials for the Kafka instance
    When you set the Kafka instance bootstrap server endpoint and service account credentials as environment variables
    Then Kcat can access all required configurations to authenticate the Kafka instance

    # 2. Producing messages in Kcat
    Given Kcat is installed
    * You have a running Kafka instance in OpenShift Streams for Apache Kafka
    * The Kafka instance is in Ready state
    * You’ve set the Kafka bootstrap server endpoint and your service account credentials as environment variables
    When you start Kcat in producer mode
    And you enter messages into Kcat that you want to produce
    Then messages are produced to the specified topic in the configured Kafka instance

    #3. Consuming messages in Kcat
    Given Kcat is installed
    * You have a running Kafka instance in OpenShift Streams for Apache Kafka
    * The instance is in Ready state
    * You’ve set the Kafka bootstrap server endpoint and your service account credentials as environment variables
    * You used a producer to produce example messages to a topic
    When you start Kcat in consumer mode
    Then messages from the producer are consumed and displayed on the command line

  Scenario: Using Kafka scripts to connect with Red Hat OpenShift Streams for Apache Kafka
    Given you have a Red Hat account
    * You have a running Kafka instance in OpenShift Streams for Apache Kafka
    * The Kafka instance has a generated bootstrap server
    * You created a service account for the Kafka instance
    * JDK 11 or later is installed
    * For Windows, the latest version of Oracle JDK is installed
    * You have downloaded and verified the latest supported binary version of the Apache Kafka distribution

    # 1. Configuring Kafka scripts to connect to a Kafka instance
    Given you have client ID and Secret for the service account, and a SASL connection mechanism
    When you create an `app-services.properties` file in the local `/config` directory of the Kafka binaries
    Then it contains the connection values

    # 2. Producing messages using Kafka scripts
    Given you are set up to produce messages to Kafka topics
    * The Kafka topic creation script `kafka-topics.sh` is available in the `/bin` directory of the Kafka binaries
    * The Kafka producer creation script `kafka-console-producer.sh` is available in the `/bin` directory of the Kafka binaries
    * An `app-services.properties` file is configured in the local `/config` directory of the Kafka binaries
    * You have the bootstrap server address for the Kafka instance
    When you enter a command to create a Kafka topic using `kafka-topics.sh`
    And use the bootstrap server address
    And use the `app-services.properties` file
    Then a topic is created in the Kafka instance
    Given you have created a topic
    When you produce messages to the topic you created using `kafka-console-producer.sh`
    And use the bootstrap server address as a parameter
    And use the `app-services.properties` file as a parameter
    Then messages are produced to the topic in the Kafka instance

    # 3. Consuming messages using Kafka scripts
    Given you are set up to consume messages
    * The Kafka consumer creation script `kafka-console-consumer.sh` is available in the `/bin` directory of the Kafka binaries
    * An `app-services.properties` file is configured in the local `/config` directory of the Kafka binaries
    * You have the bootstrap server address for the Kafka instance
    * A topic contains the messages you produced in the Kafka instance
    When you consume messages from the topic you created using `kafka-console-consumer.sh`
    And use the bootstrap server address as a parameter
    And use the `app-services.properties` file as a parameter
    Then messages are consumed from the topic
    And displayed on the command line
