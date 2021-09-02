package io.managed.services.test.client.kafkamgmt;


import com.openshift.cloud.api.kas.invoker.ApiClient;
import com.openshift.cloud.api.kas.invoker.auth.HttpBearerAuth;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.KafkaRequestPayload;
import io.managed.services.test.Environment;
import io.managed.services.test.ThrowableFunction;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiNotFoundException;
import io.managed.services.test.client.exception.ApiToManyRequestsException;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.User;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.managed.services.test.TestUtils.waitFor;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;


public class KafkaMgmtAPIUtils {
    private static final Logger LOGGER = LogManager.getLogger(KafkaMgmtAPIUtils.class);

    public static Future<KafkaMgmtApi> kafkaMgmtApi(Vertx vertx) {
        return kafkaMgmtApi(vertx, Environment.SSO_USERNAME, Environment.SSO_PASSWORD);
    }

    public static Future<KafkaMgmtApi> kafkaMgmtApi(Vertx vertx, String username, String password) {
        var auth = new KeycloakOAuth(vertx);

        LOGGER.info("authenticate user: {} against RH SSO", Environment.SSO_USERNAME);
        return auth.loginToRHSSO(username, password)
            .map(u -> kafkaMgmtApi(u));
    }

    public static KafkaMgmtApi kafkaMgmtApi(User user) {
        return kafkaMgmtApi(Environment.SERVICE_API_URI, KeycloakOAuth.getToken(user));
    }

    public static KafkaMgmtApi kafkaMgmtApi(String uri, String token) {
        var apiClient = new ApiClient();
        apiClient.setBasePath(uri);
        ((HttpBearerAuth) apiClient.getAuthentication("Bearer")).setBearerToken(token);
        return new KafkaMgmtApi(apiClient);
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

    /**
     * Create a Kafka instance using the default options if it doesn't exist or return the existing Kafka instance
     *
     * @param api  KafkaMgmtApi
     * @param name Name for the Kafka instance
     * @return KafkaRequest
     */
    public static KafkaRequest applyKafkaInstance(KafkaMgmtApi api, String name)
        throws ApiGenericException, InterruptedException, KafkaToManyRequestsException, KafkaNotReadyException {

        var payload = new KafkaRequestPayload()
            .name(name)
            .multiAz(true)
            .cloudProvider("aws")
            .region("us-east-1");

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
        throws ApiGenericException, InterruptedException, KafkaNotReadyException, KafkaToManyRequestsException {

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
        throws ApiGenericException, InterruptedException, KafkaToManyRequestsException {

        var kafkaAtom = new AtomicReference<KafkaRequest>();
        var exceptionAtom = new AtomicReference<ApiToManyRequestsException>();
        ThrowableFunction<Boolean, Boolean, ApiGenericException> ready = last -> {
            try {
                kafkaAtom.set(api.createKafka(true, payload));
            } catch (ApiToManyRequestsException e) {
                LOGGER.debug("failed to create kafka instance:", e);
                exceptionAtom.set(e);
                return false;
            }
            return true;
        };

        try {
            waitFor("create kafka instance", ofSeconds(10), ofMinutes(10), ready);
        } catch (TimeoutException e) {
            throw new KafkaToManyRequestsException(exceptionAtom.get());
        }

        return kafkaAtom.get();
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
     * Returns KafkaRequest only if status is in ready
     *
     * @param api     KafkaMgmtApi
     * @param kafkaID String
     * @return KafkaRequest
     */
    public static KafkaRequest waitUntilKafkaIsReady(KafkaMgmtApi api, String kafkaID)
        throws KafkaNotReadyException, ApiGenericException, InterruptedException {

        var kafkaAtom = new AtomicReference<KafkaRequest>();
        ThrowableFunction<Boolean, Boolean, ApiGenericException> ready = last -> {
            var kafka = api.getKafkaById(kafkaID);
            kafkaAtom.set(kafka);

            LOGGER.debug(kafka);
            return "ready".equals(kafka.getStatus());
        };

        try {
            waitFor("kafka instance to be ready", ofSeconds(10), ofMinutes(10), ready);
        } catch (TimeoutException e) {
            // throw a more accurate error
            throw new KafkaNotReadyException(kafkaAtom.get(), e);
        }

        var kafka = kafkaAtom.get();
        LOGGER.info("kafka instance '{}' is ready", kafka.getName());
        LOGGER.debug(kafka);
        return kafka;
    }

    /**
     * Return only if the Kafka instance is deleted
     *
     * @param api     KafkaMgmtApi
     * @param kafkaID Kafka instance id
     */
    public static void waitUntilKafkaIsDeleted(KafkaMgmtApi api, String kafkaID)
        throws ApiGenericException, InterruptedException, KafkaNotDeletedException {

        var kafkaAtom = new AtomicReference<KafkaRequest>();
        ThrowableFunction<Boolean, Boolean, ApiGenericException> ready = last -> {
            KafkaRequest kafka;
            try {
                kafka = api.getKafkaById(kafkaID);
            } catch (ApiNotFoundException __) {
                return true;
            }

            kafkaAtom.set(kafka);
            LOGGER.debug(kafka);
            return false;
        };

        try {
            waitFor("kafka instance to be deleted", ofSeconds(10), ofMinutes(10), ready);
        } catch (TimeoutException e) {
            throw new KafkaNotDeletedException(kafkaAtom.get(), e);
        }
    }
}
