package io.managed.services.test.client.kafkamgmt;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openshift.cloud.api.kas.invoker.ApiClient;
import com.openshift.cloud.api.kas.models.Error;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.KafkaRequestPayload;
import com.openshift.cloud.api.kas.models.KafkaUpdateRequest;
import io.managed.services.test.DNSUtils;
import io.managed.services.test.Environment;
import io.managed.services.test.ThrowingFunction;
import io.managed.services.test.ThrowingSupplier;
import io.managed.services.test.client.exception.ApiForbiddenException;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiNotFoundException;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.oauth.KeycloakUser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.managed.services.test.TestUtils.waitFor;
import static java.time.Duration.ofDays;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;


public class KafkaMgmtApiUtils {
    private static final Logger LOGGER = LogManager.getLogger(KafkaMgmtApiUtils.class);
    private static final String CLUSTER_CAPACITY_EXHAUSTED_CODE = "KAFKAS-MGMT-24";

    public static KafkaMgmtApi kafkaMgmtApi(String uri, KeycloakUser user) {
        return new KafkaMgmtApi(new ApiClient().setBasePath(uri), user);
    }

    /**
     * Get Kafka by name or return empty optional
     *
     * @param api  KafkaMgmtApi
     * @param name Kafka Instance name
     * @return Optional KafkaRequest
     */
    public static Optional<KafkaRequest> getKafkaByName(KafkaMgmtApi api, String name) throws ApiGenericException {
        var list = api.getKafkas("1", "1", null, String.format("name = %s", name.trim()));
        return list.getItems().stream().findAny();
    }

    public static KafkaRequestPayload defaultKafkaInstance(String name) {
        return new KafkaRequestPayload()
            .name(name)
            .cloudProvider("gcp")
            .region(Environment.DEFAULT_KAFKA_REGION);
    }

    /**
     * Create a Kafka instance using the default options if it doesn't exist or return the existing Kafka instance
     *
     * @param api  KafkaMgmtApi
     * @param name Name for the Kafka instance
     * @return KafkaRequest
     */
    public static KafkaRequest applyKafkaInstance(KafkaMgmtApi api, String name)
        throws ApiGenericException, InterruptedException, KafkaClusterCapacityExhaustedException, KafkaNotReadyException, KafkaUnknownHostsException, KafkaUnprovisionedException {

        var payload = defaultKafkaInstance(name);
        return applyKafkaInstance(api, payload);
    }

    /**
     * Create a Kafka instance if it doesn't exist or return the existing Kafka instance
     *
     * @param api     KafkaMgmtApi
     * @param payload CreateKafkaPayload
     * @return KafkaRequest
     */
    public static KafkaRequest applyKafkaInstance(KafkaMgmtApi api, KafkaRequestPayload payload)
        throws ApiGenericException, InterruptedException, KafkaNotReadyException, KafkaClusterCapacityExhaustedException, KafkaUnknownHostsException, KafkaUnprovisionedException {

        var existing = getKafkaByName(api, payload.getName());

        KafkaRequest kafka;
        if (existing.isPresent()) {
            kafka = existing.get();
            LOGGER.warn("kafka instance '{}' already exists", kafka.getName());
            LOGGER.debug(kafka);
        } else {
            LOGGER.info("create kafka instance '{}'", payload.getName());
            kafka = createKafkaInstance(api, payload);
        }

        if (List.of("accepted", "preparing", "provisioning", "failed").contains(kafka.getStatus())) {
            return waitUntilKafkaIsReady(api, kafka.getId());
        }
        if ("ready".equals(kafka.getStatus())) {
            return kafka;
        }
        throw new KafkaNotReadyException(kafka);
    }

    /**
     * Create a Kafka instance but retry for 10 minutes if the cluster capacity is exhausted.
     *
     * @param api     KafkaMgmtApi
     * @param payload CreateKafkaPayload
     * @return KafkaRequest
     */
    public static KafkaRequest createKafkaInstance(KafkaMgmtApi api, KafkaRequestPayload payload)
        throws ApiGenericException, InterruptedException, KafkaClusterCapacityExhaustedException, KafkaUnprovisionedException {

        var kafkaAtom = new AtomicReference<KafkaRequest>();
        var exceptionAtom = new AtomicReference<ApiForbiddenException>();
        ThrowingFunction<Boolean, Boolean, ApiGenericException> ready = last -> {
            try {
                kafkaAtom.set(api.createKafka(true, payload));
            } catch (ApiForbiddenException e) {

                Error error;
                try {
                    error = new ObjectMapper().readValue(e.getResponseBody(), Error.class);
                } catch (JsonProcessingException ex) {
                    LOGGER.warn("failed to decode API error: ", ex);
                    throw e;
                }

                if (CLUSTER_CAPACITY_EXHAUSTED_CODE.equals(error.getCode())) {
                    // try again without logging
                    exceptionAtom.set(e);
                    LOGGER.debug("{}: {}", e.getClass(), e.getMessage());
                    return false;
                }

                // failed for other reasons
                throw e;
            }
            return true;
        };

        try {
            waitFor("create kafka instance", ofSeconds(30), ofDays(1), ready);
        } catch (TimeoutException e) {
            throw new KafkaClusterCapacityExhaustedException(exceptionAtom.get());
        }

        // If there is space in other regions but not in the requested region the Kafka instance
        // remains in the accepted state until a space doesn't become available in the requested region
        // Workaround for https://issues.redhat.com/browse/MGDSTRM-5995
        return waitUntilKafkaIsProvisioning(api, kafkaAtom.get().getId());
    }

    /**
     * Delete the Kafka Instance if it exists and if the SKIP_KAFKA_TEARDOWN env is set to false.
     *
     * @param api  KafkaMgmtApi
     * @param name Kafka Instance name
     */
    public static void cleanKafkaInstance(KafkaMgmtApi api, String name) throws ApiGenericException {
        if (Environment.SKIP_KAFKA_TEARDOWN) {
            LOGGER.warn("skip kafka instance clean up");
            return;
        }
        deleteKafkaByNameIfExists(api, name);
    }

    /**
     * Delete Kafka Instance by name if it exists
     *
     * @param api  KafkaMgmtApi
     * @param name Kafka Instance name
     */
    public static void deleteKafkaByNameIfExists(KafkaMgmtApi api, String name) throws ApiGenericException {

        var exists = getKafkaByName(api, name);
        if (exists.isPresent()) {
            var kafka = exists.get();
            LOGGER.info("delete kafka instance '{}'", kafka.getName());
            LOGGER.debug(kafka);
            api.deleteKafkaById(kafka.getId(), true);
            LOGGER.info("kafka instance '{}' deleted", kafka.getName());
        } else {
            LOGGER.info("kafka instance '{}' not found", name);
        }
    }

    /**
     * Returns KafkaRequest only if status is in provisioning
     *
     * @param api     KafkaMgmtApi
     * @param kafkaID String
     * @return KafkaRequest
     */
    public static KafkaRequest waitUntilKafkaIsProvisioning(KafkaMgmtApi api, String kafkaID)
        throws KafkaUnprovisionedException, ApiGenericException, InterruptedException {

        var kafkaAtom = new AtomicReference<KafkaRequest>();
        ThrowingFunction<Boolean, Boolean, ApiGenericException> ready = last -> {
            var kafka = api.getKafkaById(kafkaID);
            kafkaAtom.set(kafka);

            LOGGER.debug(kafka);
            return !"accepted".equals(kafka.getStatus());
        };

        try {
            waitFor("kafka instance to to start provisioning", ofSeconds(30), ofDays(1), ready);
        } catch (TimeoutException e) {
            // throw a more accurate error
            throw new KafkaUnprovisionedException(kafkaAtom.get(), e);
        }

        var kafka = kafkaAtom.get();
        LOGGER.info("kafka instance '{}' is provisioning", kafka.getName());
        LOGGER.debug(kafka);

        return kafka;
    }


    /**
     * Returns KafkaRequest only if status is in ready
     *
     * @param api     KafkaMgmtApi
     * @param kafkaID String
     * @return KafkaRequest
     */
    public static KafkaRequest waitUntilKafkaIsReady(KafkaMgmtApi api, String kafkaID)
        throws KafkaNotReadyException, ApiGenericException, InterruptedException, KafkaUnknownHostsException {

        return waitUntilKafkaIsReady(() -> api.getKafkaById(kafkaID));
    }


    /**
     * Returns KafkaRequest only if status is in ready
     *
     * @param supplier Returns the kafka instance to wait for
     * @return KafkaRequest
     */
    public static <T extends Throwable> KafkaRequest waitUntilKafkaIsReady(ThrowingSupplier<KafkaRequest, T> supplier)
        throws T, InterruptedException, KafkaUnknownHostsException, KafkaNotReadyException {

        var kafkaAtom = new AtomicReference<KafkaRequest>();
        ThrowingFunction<Boolean, Boolean, T> ready = last -> {
            var kafka = supplier.get();
            kafkaAtom.set(kafka);

            LOGGER.debug(kafka);
            return "ready".equals(kafka.getStatus());
        };

        try {
            waitFor("kafka instance to be ready", ofSeconds(10), ofMinutes(30), ready);
        } catch (TimeoutException e) {
            // throw a more accurate error
            throw new KafkaNotReadyException(kafkaAtom.get(), e);
        }

        var kafka = kafkaAtom.get();
        LOGGER.info("kafka instance '{}' is ready", kafka.getName());
        LOGGER.debug(kafka);

        waitUntilKafkaHostsAreResolved(kafka);

        return kafka;
    }


    public static void waitUntilKafkaHostsAreResolved(KafkaRequest kafka)
        throws InterruptedException, KafkaUnknownHostsException {

        var bootstrapHost = Objects.requireNonNull(kafka.getBootstrapServerHost());
        var bootstrap = bootstrapHost.replaceFirst(":443$", "");
        var broker0 = "broker-0-" + bootstrap;
        var broker1 = "broker-1-" + bootstrap;
        var broker2 = "broker-2-" + bootstrap;
        var admin = "admin-server-" + bootstrap;
        var hosts = new ArrayList<>(List.of(bootstrap, admin, broker0, broker1, broker2));

        // if Kafka instance is of type developer it is smaller and does not have broker1 and broker 2
        if (Objects.requireNonNull(kafka.getInstanceType()).equals("developer")) {
            hosts.removeAll(List.of(broker1, broker2));
        }

        ThrowingFunction<Boolean, Boolean, java.lang.Error> ready = last -> {

            for (var i = 0; i < hosts.size(); i++) {
                try {
                    var r = InetAddress.getByName(hosts.get(i));
                    LOGGER.info("host '{}' resolved wit address '{}'", hosts.get(i), r.getHostAddress());

                    // remove resolved hosts from the list
                    hosts.remove(i);
                    i--; // shift i back to not skip a host
                } catch (UnknownHostException e) {
                    LOGGER.debug("failed to resolve host '{}': {}", hosts.get(i), e.getMessage());

                    // TODO: Move to trace with isTrace enable
                    LOGGER.debug("dig {}:\n{}", hosts.get(i), DNSUtils.dig(hosts.get(i)));
                    LOGGER.debug("dig @1.1.1.1 {}:\n{}", hosts.get(i), DNSUtils.dig(hosts.get(i), "1.1.1.1"));
                }
            }
            return hosts.isEmpty();
        };

        try {
            waitFor("kafka hosts to be resolved", ofSeconds(5), ofMinutes(5), ready);
        } catch (TimeoutException e) {
            throw new KafkaUnknownHostsException(hosts, e);
        }

        LOGGER.debug("kafka hosts '{}' are ready", hosts);
    }

    /**
     * Return only if the Kafka instance is deleted
     *
     * @param api     KafkaMgmtApi
     * @param kafkaID Kafka instance id
     */
    public static void waitUntilKafkaIsDeleted(KafkaMgmtApi api, String kafkaID)
        throws ApiGenericException, InterruptedException, KafkaNotDeletedException {

        waitUntilKafkaIsDeleted(() -> {
            try {
                return Optional.of(api.getKafkaById(kafkaID));
            } catch (ApiNotFoundException __) {
                return Optional.empty();
            }
        });
    }

    /**
     * Return only if the Kafka instance is deleted
     *
     * @param supplier Return true if the instance doesn't exist anymore
     */
    public static <T extends Throwable> void waitUntilKafkaIsDeleted(
        ThrowingSupplier<Optional<KafkaRequest>, T> supplier)
        throws T, InterruptedException, KafkaNotDeletedException {

        var kafkaAtom = new AtomicReference<KafkaRequest>();
        ThrowingFunction<Boolean, Boolean, T> ready = l -> {
            var exists = supplier.get();
            if (exists.isEmpty()) {
                return true;
            }

            var kafka = exists.get();
            LOGGER.debug(kafka);
            kafkaAtom.set(kafka);
            return false;
        };

        try {
            waitFor("kafka instance to be deleted", ofSeconds(10), ofMinutes(10), ready);
        } catch (TimeoutException e) {
            throw new KafkaNotDeletedException(kafkaAtom.get(), e);
        }
    }

    /**
     * Wait for the new owner to be applied to all brokers.
     *
     * Attention: This method will try to create and delete a topic using the new owner, if the
     * new owner has explicit deny or allow ACLs for topic creation and deletion, this method will
     * fail in case of explicit deny or succeed without waiting in case of explicit allow.
     *
     * Note: This is a workaround until the KafkaResponse object will not expose the real owner or a
     * status that can be used to determinate when the owner switch is completed.
     *
     * @param newOwnerKafkaInstanceApi The KafkaInstanceApi created for the new instance owner
     */
    public static void waitUntilOwnerIsChanged(KafkaInstanceApi newOwnerKafkaInstanceApi)
        throws TimeoutException, ApiGenericException, InterruptedException {

        var topicName = "topic-used-to-wait";
        ThrowingFunction<Boolean, Boolean, ApiGenericException> ready = l -> {
            // catches (ApiForbiddenException) while waiting for becoming Authorized (i.e., Owner), and also problem with replication factor when Rollout takes place.
            try {
                KafkaInstanceApiUtils.applyTopic(newOwnerKafkaInstanceApi, topicName);
                // temporary topic is cleaned afterwards
                newOwnerKafkaInstanceApi.deleteTopic(topicName);
                return true;
            } catch (ApiGenericException e) {
                LOGGER.debug(e);
                return false;
            }
        };

        try {
            waitFor("kafka owner to be changed", ofSeconds(10), ofMinutes(5), ready);
        } catch (TimeoutException e) {
            // When the owner change all the Kafka brokers need to be redeployed and this could take some time, but we expect it to be completed within 5 minutes
            throw new TimeoutException("kafka instance did not switch the owner (waiting for rollback), within expected time");
        }
    }

    /**
     * Change the owner of a Kafka instance.
     * <p>
     * Note: The change is async and the waitUntilOwnerIsChanged function should be used
     * to make sure the new owner has been changed
     *
     * @param mgmtApi      KafkaMgmtApi
     * @param kafka        Kafka instance to update
     * @param newOwnerName The name of the new owner
     * @return KafkaRequest
     */
    public static KafkaRequest changeKafkaInstanceOwner(KafkaMgmtApi mgmtApi, KafkaRequest kafka, String newOwnerName) throws Throwable {

        var kafkaUpdateRequest = new KafkaUpdateRequest()
            .owner(newOwnerName.toLowerCase(Locale.ROOT));

        return mgmtApi.updateKafka(kafka.getId(), kafkaUpdateRequest);
    }

    /**
     * Get total partition limit of given kafka instance.
     *
     * @param api      KafkaMgmtApi
     * @param kafka    Kafka instance to query
     */
    public static int getPartitionLimitMax(KafkaMgmtApi api, KafkaRequest kafka) throws Exception {
        return getMetric(api, kafka, "^kafka_instance_partition_limit.*\\s(\\d+)$");
    }

    /**
     * Get message size limit of given kafka instance.
     *
     * @param api      KafkaMgmtApi
     * @param kafka    Kafka instance to query
     */
    public static int getMessageSizeLimit(KafkaMgmtApi api, KafkaRequest kafka) throws Exception {
        return getMetric(api, kafka, "^kafka_instance_max_message_size_limit.*\\s(\\d+)$");
    }

    /**
     * Get desired broker count of given kafka instance.
     *
     * @param api      KafkaMgmtApi
     * @param kafka    Kafka instance to query
     */
    public static int getDesiredBrokerCount(KafkaMgmtApi api, KafkaRequest kafka) throws Exception {
        return getMetric(api, kafka, "^kafka_instance_spec_brokers_desired_count.*\\s(\\d+)$");
    }

    /**
     * Get total partition limit of per given kafka instance.
     *
     * @param api      KafkaMgmtApi
     * @param kafka    Kafka instance to query
     * @param regex    metric pattern
     */
    private static int getMetric(KafkaMgmtApi api, KafkaRequest kafka, String regex) throws Exception {
        var metrics = api.federateMetrics(kafka.getId());
        final Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);
        final Matcher matcher = pattern.matcher(metrics);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        // if not found
        throw new Exception(String.format("Unable to find metric matching %s", regex));
    }
}
