package io.managed.services.test.quickstarts;

import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.managed.services.test.Environment;
import io.managed.services.test.client.kafkaadminapi.KafkaAdminAPI;
import io.managed.services.test.client.kafkaadminapi.KafkaAdminAPIUtils;
import io.managed.services.test.client.serviceapi.CreateKafkaPayload;
import io.managed.services.test.client.serviceapi.CreateServiceAccountPayload;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.client.serviceapi.ServiceAccount;
import io.managed.services.test.kafka.KafkaManagerAPITest;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteKafkaByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteServiceAccountByNameIfExists;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.waitUntilKafkaIsReady;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class QuickstartsStepDefinitions {
    private static final Logger LOGGER = LogManager.getLogger(KafkaManagerAPITest.class);

    private static final String KAFKA_INSTANCE_NAME = "cucumber-qs-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String SERVICE_ACCOUNT_NAME = "cucumber-sa-qs-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String TOPIC_NAME = "test-topic";


    private final Vertx vertx = Vertx.vertx();

    private ServiceAPI api;
    private KafkaResponse kafkaInstance;
    private ServiceAccount serviceAccount;
    private KafkaAdminAPI kafkaInstanceAdmin;

    @Given("I am authenticated to Red Hat SSO")
    public void i_am_authenticated_to_red_hat_sso() throws Throwable {
        api = bwait(ServiceAPIUtils.serviceAPI(vertx));
    }

    @When("I create a Kafka instance with a unique name")
    public void i_create_a_kafka_instance_with_a_unique_name() throws Throwable {
        CreateKafkaPayload kafkaPayload = ServiceAPIUtils.createKafkaPayload(KAFKA_INSTANCE_NAME);
        kafkaInstance = bwait(ServiceAPIUtils.createKafkaInstance(vertx, api, kafkaPayload));
    }

    @Then("the Kafka instance is listed in the instances table")
    public void the_kafka_instance_is_listed_in_the_instances_table() throws Throwable {
        var list = bwait(api.getListOfKafkas());
        var o = list.items.stream().filter(k -> k.name.equals(KAFKA_INSTANCE_NAME)).findAny();
        assertTrue(o.isPresent());
    }

    @Then("the Kafka instance is shown as Ready")
    public void the_kafka_instance_is_shown_as_ready() throws Throwable {
        kafkaInstance = bwait(waitUntilKafkaIsReady(vertx, api, kafkaInstance.id));
        assertEquals(kafkaInstance.status, "ready");
    }

    @Then("the Kafka instance has a generated Bootstrap server")
    public void the_kafka_instance_has_a_generated_bootstrap_server() {
        assertNotNull(kafkaInstance.bootstrapServerHost);
    }

    @When("I create a service account with a unique name")
    public void i_create_a_service_account_with_a_unique_name() throws Throwable {
        // Create Service Account
        CreateServiceAccountPayload serviceAccountPayload = new CreateServiceAccountPayload();
        serviceAccountPayload.name = SERVICE_ACCOUNT_NAME;
        serviceAccount = bwait(api.createServiceAccount(serviceAccountPayload));
    }

    @Then("the service account has a generated Client ID and Client Secret")
    public void the_service_account_has_a_generated_client_id_and_client_secret() {
        assertNotNull(serviceAccount.clientID);
        assertNotNull(serviceAccount.clientSecret);
    }

    @Then("the service account is listed in the service accounts table")
    public void the_service_account_is_listed_in_the_service_accounts_table() throws Throwable {
        var list = bwait(api.getListOfServiceAccounts());
        var o = list.items.stream().filter(a -> a.name.equals(SERVICE_ACCOUNT_NAME)).findAny();
        assertTrue(o.isPresent());
    }

    @Given("a ready Kafka instance")
    public void a_ready_kafka_instance() {
        assertEquals(kafkaInstance.status, "ready");
    }

    @When("I create a Kafka topic with a unique name")
    public void i_create_a_kafka_topic_with_a_unique_name() throws Throwable {
        kafkaInstanceAdmin = bwait(KafkaAdminAPIUtils.kafkaAdminAPI(vertx, kafkaInstance.bootstrapServerHost));
        bwait(KafkaAdminAPIUtils.createDefaultTopic(kafkaInstanceAdmin, TOPIC_NAME));
    }

    @Then("the Kafka topic is listed in the topics table")
    public void the_kafka_topic_is_listed_in_the_topics_table() throws Throwable {
        var l = bwait(kafkaInstanceAdmin.getAllTopics());
        var o = l.items.stream().filter(t -> t.name.equals(TOPIC_NAME)).findAny();
        assertTrue(o.isPresent());
    }

    @After
    public void teardown() throws Throwable {
        // delete kafka instance
        try {
            bwait(deleteKafkaByNameIfExists(api, KAFKA_INSTANCE_NAME));
        } catch (Throwable t) {
            LOGGER.error("clean main kafka instance error: ", t);
        }

        // delete service account
        try {
            bwait(deleteServiceAccountByNameIfExists(api, SERVICE_ACCOUNT_NAME));
        } catch (Throwable t) {
            LOGGER.error("clean service account error: ", t);
        }

        // close vertx
        bwait(vertx.close());
    }
}
