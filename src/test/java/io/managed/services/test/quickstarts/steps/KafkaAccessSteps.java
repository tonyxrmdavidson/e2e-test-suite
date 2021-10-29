package io.managed.services.test.quickstarts.steps;

import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.quickstarts.contexts.KafkaInstanceContext;
import io.managed.services.test.quickstarts.contexts.ServiceAccountContext;
import lombok.extern.log4j.Log4j2;

import static io.managed.services.test.TestUtils.assumeTeardown;

@Log4j2
public class KafkaAccessSteps {

    private final KafkaInstanceContext kafkaInstanceContext;
    private final ServiceAccountContext serviceAccountContext;

    // If access is created for this principal it will be clean in the teardown
    private String principal;

    public KafkaAccessSteps(KafkaInstanceContext kafkaInstanceContext, ServiceAccountContext serviceAccountContext) {
        this.kafkaInstanceContext = kafkaInstanceContext;
        this.serviceAccountContext = serviceAccountContext;
    }

    @Given("you have set the permissions for your service account to access the Kafka instance resources")
    public void you_have_set_the_permissions_for_your_service_account_to_access_the_kafka_instance_resources() throws Throwable {
        var kafkaInstanceApi = kafkaInstanceContext.kafkaInstanceApi();
        var serviceAccount = serviceAccountContext.requireServiceAccount();

        var principal = KafkaInstanceApiUtils.toPrincipal(serviceAccount.getClientId());
        log.info("apply producer and consumer ACLs for principal '{}'", principal);
        KafkaInstanceApiUtils.applyProducerAndConsumerACLs(kafkaInstanceApi, principal);

        this.principal = principal;
    }

    @After(order = 10100)
    public void teardown() {
        assumeTeardown();

        if (principal != null) {
            try {
                var kafkaMgmtApi = kafkaInstanceContext.kafkaInstanceApi();
                log.info("clean acls for principal '{}'", principal);
                kafkaMgmtApi.deleteAcls(null, null, null, principal, null, null);
            } catch (Throwable t) {
                log.error("failed to clean access:", t);
            }

            principal = null;
        }
    }
}
