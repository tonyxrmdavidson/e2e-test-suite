Feature: Quickstarts

  Scenario: Getting started with Red Hat OpenShift Streams for Apache Kafka
    # 1. Creating a Kafka instance in OpenShift Streams for Apache Kafka
    When I create a Kafka instance with a unique name
    Then the Kafka instance is listed in the instances table
    Then the Kafka instance is shown as Ready

    # 2. Creating a service account to connect to a Kafka instance in OpenShift Streams for Apache Kafka
    Then the Kafka instance has a generated Bootstrap server

    When I create a service account with a unique name
    Then the service account has a generated Client ID and Client Secret
    Then the service account is listed in the service accounts table

    # 3. Creating a Kafka topic in OpenShift Streams for Apache Kafka
    Given a ready Kafka instance
    When I create a Kafka topic with a unique name
    Then the Kafka topic is listed in the topics table
