package io.managed.services.test.registry;


import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import com.openshift.cloud.api.srs.models.RegistryRest;
import io.apicurio.registry.serde.SerdeConfig;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.kafka.AvroKafkaGenericDeserializer;
import io.managed.services.test.client.kafka.AvroKafkaGenericSerializer;
import io.managed.services.test.client.kafka.KafkaAuthMethod;
import io.managed.services.test.client.kafka.KafkaConsumerClient;
import io.managed.services.test.client.kafka.KafkaProducerClient;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.managed.services.test.client.registrymgmt.RegistryMgmtApi;
import io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.List;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static org.testng.Assert.assertEquals;

@Test(groups = TestTag.REGISTRY)
public class RegistryKafkaIntegrationTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(RegistryKafkaIntegrationTest.class);

    private static final String KAFKA_INSTANCE_NAME = "mk-e2e-ki-rki-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String SERVICE_REGISTRY_NAME = "mk-e2e-sr-rki-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String SERVICE_ACCOUNT_NAME = "mk-e2e-sa-rki-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String TOPIC_NAME = "test-topic";
    private static final String ARTIFACT_SCHEMA = "{\"type\":\"record\",\"name\":\"Greeting\",\"fields\":[{\"name\":\"Message\",\"type\":\"string\"},{\"name\":\"Time\",\"type\":\"long\"}]}";

    private final Vertx vertx = Vertx.vertx();

    private RegistryMgmtApi registryMgmtApi;
    private RegistryRest registry;
    private KafkaMgmtApi kafkaMgmtApi;
    private SecurityMgmtApi securityMgmtApi;
    private KafkaRequest kafka;
    private ServiceAccount serviceAccount;

    @BeforeClass(timeOut = 20 * MINUTES)
    public void bootstrap() throws Throwable {

        var oauth = new KeycloakOAuth(vertx, Environment.SSO_USERNAME, Environment.SSO_PASSWORD);

        // registry api
        LOGGER.info("initialize registry, kafka security services apis");
        var apis = ApplicationServicesApi.applicationServicesApi(oauth, Environment.SERVICE_API_URI);
        registryMgmtApi = apis.registryMgmt();
        kafkaMgmtApi = apis.kafkaMgmt();
        securityMgmtApi = apis.securityMgmt();

        // registry
        LOGGER.info("create service registry: {}", SERVICE_REGISTRY_NAME);
        registry = RegistryMgmtApiUtils.applyRegistry(registryMgmtApi, SERVICE_REGISTRY_NAME);

        // kafka
        LOGGER.info("create kafka instance: {}", KAFKA_INSTANCE_NAME);
        kafka = KafkaMgmtApiUtils.applyKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        LOGGER.debug(kafka);

        // service account
        LOGGER.info("create service account: {}", SERVICE_ACCOUNT_NAME);
        serviceAccount = SecurityMgmtAPIUtils.applyServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME);

        // topic
        LOGGER.info("create topic: {}", TOPIC_NAME);
        var kafkaInstanceApi = bwait(KafkaInstanceApiUtils.kafkaInstanceApi(oauth, kafka));
        var topic = KafkaInstanceApiUtils.applyTopic(kafkaInstanceApi, TOPIC_NAME);
        LOGGER.debug(topic);
    }

    @AfterClass(timeOut = DEFAULT_TIMEOUT, alwaysRun = true)
    public void teardown() throws Throwable {
        assumeTeardown();

        try {
            KafkaMgmtApiUtils.cleanKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            LOGGER.error("clan kafka error: ", t);
        }

        try {
            SecurityMgmtAPIUtils.cleanServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean service account error: ", t);
        }

        try {
            RegistryMgmtApiUtils.cleanRegistry(registryMgmtApi, SERVICE_REGISTRY_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean service registry error: ", t);
        }

        bwait(vertx.close());
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testProduceConsumeAvroMessageWithServiceRegistry() throws Throwable {

        // producer
        LOGGER.info("initialize producer with registry");
        var producerRegistryConfig = new HashMap<String, String>();
        producerRegistryConfig.put(SerdeConfig.REGISTRY_URL, registry.getRegistryUrl());
        producerRegistryConfig.put(SerdeConfig.AUTO_REGISTER_ARTIFACT, "true");
        producerRegistryConfig.put(SerdeConfig.AUTO_REGISTER_ARTIFACT_IF_EXISTS, "RETURN");
        producerRegistryConfig.put(SerdeConfig.AUTH_USERNAME, serviceAccount.getClientId());
        producerRegistryConfig.put(SerdeConfig.AUTH_PASSWORD, serviceAccount.getClientSecret());

        var producer = new KafkaProducerClient<>(
            vertx,
            kafka.getBootstrapServerHost(),
            serviceAccount.getClientId(),
            serviceAccount.getClientSecret(),
            KafkaAuthMethod.OAUTH,
            StringSerializer.class,
            AvroKafkaGenericSerializer.class,
            producerRegistryConfig);

        // consumer
        LOGGER.info("initialize consumer with registry");
        var consumerRegistryConfig = new HashMap<String, String>();
        consumerRegistryConfig.put(SerdeConfig.REGISTRY_URL, registry.getRegistryUrl());
        consumerRegistryConfig.put(SerdeConfig.AUTH_USERNAME, serviceAccount.getClientId());
        consumerRegistryConfig.put(SerdeConfig.AUTH_PASSWORD, serviceAccount.getClientSecret());

        var consumer = new KafkaConsumerClient<>(
            vertx,
            kafka.getBootstrapServerHost(),
            serviceAccount.getClientId(),
            serviceAccount.getClientSecret(),
            KafkaAuthMethod.OAUTH,
            "test-group",
            "latest",
            StringDeserializer.class,
            AvroKafkaGenericDeserializer.class,
            consumerRegistryConfig);

        var schema = new Schema.Parser().parse(ARTIFACT_SCHEMA);

        LOGGER.info("prepare the record to send");
        var record = new GenericData.Record(schema);
        var now = new Date();
        record.put("Message", "Hello World");
        record.put("Time", now.getTime());

        LOGGER.info("start the consumer");
        var futureRecords = bwait(consumer.receiveAsync(TOPIC_NAME, 1));

        LOGGER.info("produce the record");
        bwait(producer.sendAsync(TOPIC_NAME, List.of(record)));

        LOGGER.info("consumer the record");
        var records = bwait(futureRecords);

        LOGGER.info("Records:");
        for (var r : records) {
            LOGGER.info("  K: {}; V: {}", r.record().key(), r.record().value().get("Message"));
        }

        var m = (Utf8) records.get(0).record().value().get("Message");
        assertEquals(m.toString(), "Hello World");

        bwait(consumer.asyncClose());
        bwait(producer.asyncClose());
    }
}
