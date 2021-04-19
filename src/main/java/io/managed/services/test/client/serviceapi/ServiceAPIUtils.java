package io.managed.services.test.client.serviceapi;


import io.managed.services.test.Environment;
import io.managed.services.test.IsReady;
import io.managed.services.test.client.exception.ResponseException;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import java.util.Optional;
import java.util.stream.Collectors;

import static io.managed.services.test.TestUtils.waitFor;
import static io.vertx.core.Future.succeededFuture;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;


public class ServiceAPIUtils {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPIUtils.class);

    public static Future<ServiceAPI> serviceAPI(Vertx vertx) {
        return serviceAPI(vertx, Environment.SSO_USERNAME, Environment.SSO_PASSWORD);
    }

    public static Future<ServiceAPI> serviceAPI(Vertx vertx, String username, String password) {
        var auth = new KeycloakOAuth(vertx);

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        return auth.login(
                Environment.SSO_REDHAT_KEYCLOAK_URI,
                Environment.SSO_REDHAT_REDIRECT_URI,
                Environment.SSO_REDHAT_REALM,
                Environment.SSO_REDHAT_CLIENT_ID,
                username, password)

                .map(user -> new ServiceAPI(vertx, Environment.SERVICE_API_URI, user));
    }


    /**
     * Get Kafka by name or return empty optional
     *
     * @param api  ServiceAPI
     * @param name Kafka Instance name
     * @return Future<Optional < KafkaResponse>>
     */
    public static Future<Optional<KafkaResponse>> getKafkaByName(ServiceAPI api, String name) {
        return api.getListOfKafkaByName(name)
                .map(r -> r.items.size() == 1 ? r.items.get(0) : null)
                .map(Optional::ofNullable);
    }

    /**
     * Get Service Account by name or return empty optional
     *
     * @param api  ServiceAPI
     * @param name Service Account name
     * @return Future<Optional < ServiceAccount>>
     */
    public static Future<Optional<ServiceAccount>> getServiceAccountByName(ServiceAPI api, String name) {
        return api.getListOfServiceAccounts()
                .map(r -> r.items.stream().filter(a -> a.name.equals(name)).findFirst());
    }

    /**
     * Create a Kafka instance using the default options if it doesn't exists or return the existing Kafka instance
     *
     * @param vertx Vertx
     * @param api   ServiceAPI
     * @param name  Name for the Kafka instance
     * @return Future<KafkaResponse>
     */
    public static Future<KafkaResponse> applyKafkaInstance(Vertx vertx, ServiceAPI api, String name) {

        CreateKafkaPayload payload = new CreateKafkaPayload();
        payload.name = name;
        payload.multiAZ = true;
        payload.cloudProvider = "aws";
        payload.region = "us-east-1";

        return applyKafkaInstance(vertx, api, payload);
    }

    /**
     * Create a Kafka instance if it doesn't exists or return the existing Kafka instance
     *
     * @param vertx   Vertx
     * @param api     ServiceAPI
     * @param payload CreateKafkaPayload
     * @return Future<KafkaResponse>
     */
    public static Future<KafkaResponse> applyKafkaInstance(Vertx vertx, ServiceAPI api, CreateKafkaPayload payload) {
        return getKafkaByName(api, payload.name)
                .compose(o -> o.map(k -> {
                    LOGGER.warn("kafka instance already exists: {}", Json.encode(k));
                    return succeededFuture(k);

                }).orElseGet(() -> {
                    LOGGER.info("create kafka instance: {}", payload.name);
                    return api.createKafka(payload, true)
                            .compose(k -> waitUntilKafkaIsReady(vertx, api, k.id));
                }))
                .onSuccess(k -> LOGGER.info("apply kafka instance: {}", Json.encode(k)));
    }

    /**
     * Delete Kafka Instance by name if it exists
     *
     * @param api  ServiceAPI
     * @param name Service Account name
     * @return Future<Void>
     */
    public static Future<Void> deleteKafkaByNameIfExists(ServiceAPI api, String name) {

        return getKafkaByName(api, name)
                .compose(o -> o.map(k -> {
                    LOGGER.info("clean kafka instance: {}", k.id);
                    return api.deleteKafka(k.id, true);
                }).orElseGet(() -> {
                    LOGGER.warn("kafka instance '{}' not found", name);
                    return Future.succeededFuture();
                }));
    }

    /**
     * Delete Service Account by name if it exists
     *
     * @param api  ServiceAPI
     * @param name Service Account name
     * @return Future<Void>
     */
    public static Future<Void> deleteServiceAccountByNameIfExists(ServiceAPI api, String name) {

        return api.getListOfServiceAccounts()

                // delete all service accounts with the same name
                .map(r -> r.items.stream()
                        .filter(a -> a.name.equals(name))
                        .map(a -> {
                            LOGGER.info("clean service account: {}", a.id);
                            return (Future) api.deleteServiceAccount(a.id);
                        })
                        .collect(Collectors.toList())
                )
                .compose(l -> CompositeFuture.join(l))

                .compose(__ -> Future.succeededFuture());
    }

    /**
     * Delete Service Account by owner if it exists
     *
     * @param api   ServiceAPI
     * @param owner Service Account name
     * @return Future<Void>
     */
    public static Future<Void> deleteServiceAccountsByOwnerIfExists(ServiceAPI api, String owner) {
        return api.getListOfServiceAccounts()
                // delete all service accounts with the same name
                .map(r -> r.items.stream()
                        .filter(a -> a.owner.equals(owner))
                        .map(a -> {
                            LOGGER.info("clean service account: {}", a.id);
                            return (Future) api.deleteServiceAccount(a.id);
                        })
                        .collect(Collectors.toList())
                )
                .compose(l -> CompositeFuture.join(l))
                .compose(__ -> Future.succeededFuture());
    }

    /**
     * Function that returns kafkaResponse only if status is in ready
     *
     * @param vertx   Vertx
     * @param api     ServiceAPI
     * @param kafkaID String
     * @return KafkaResponse
     */
    public static Future<KafkaResponse> waitUntilKafkaIsReady(Vertx vertx, ServiceAPI api, String kafkaID) {
        IsReady<KafkaResponse> isReady = last -> api.getKafka(kafkaID)
                .compose(r -> isKafkaReady(r, last));

        return waitFor(vertx, "kafka instance to be ready", ofSeconds(10), ofMillis(Environment.WAIT_READY_MS), isReady);
    }

    public static Future<Pair<Boolean, KafkaResponse>> isKafkaReady(KafkaResponse kafka, boolean last) {
        LOGGER.info("kafka instance status is: {}", kafka.status);

        if (last) {
            LOGGER.warn("last kafka response is: {}", Json.encode(kafka));
        }

        if ("ready".equals(kafka.status)) {
            return Future.succeededFuture(Pair.with(true, kafka));
        }
        return Future.succeededFuture(Pair.with(false, null));
    }

    public static Future<Void> waitUntilKafkaIsDeleted(Vertx vertx, ServiceAPI api, String kafkaID) {

        IsReady<Void> isDeleted = last -> api.getKafka(kafkaID)
                .recover(throwable -> {
                    if (throwable instanceof ResponseException && ((ResponseException) throwable).response.statusCode() == 404) {
                        return Future.succeededFuture(null);
                    }
                    return Future.failedFuture(throwable);
                })
                .map(r -> {
                    LOGGER.info("Kafka response : {}", Json.encode(r));
                    return Pair.with(r == null, null);
                });

        return waitFor(vertx, "kafka instance to be deleted", ofSeconds(10), ofMillis(Environment.WAIT_READY_MS), isDeleted);
    }

    /**
     * If the service account with the passed name doesn't exists, recreate it, otherwise reset the credentials
     * and return the ServiceAccount with clientSecret
     *
     * @param api  ServiceAPI
     * @param name Service Account Name
     * @return ServiceAccount with clientSecret
     */
    public static Future<ServiceAccount> applyServiceAccount(ServiceAPI api, String name) {

        return getServiceAccountByName(api, name)
                .compose(o -> o
                        .map(v -> {
                            LOGGER.info("reset credentials for service account: {}", name);
                            return api.resetCredentialsServiceAccount(v.id);
                        })
                        .orElseGet(() -> {
                            LOGGER.warn("create service account: {}", name);

                            var serviceAccountPayload = new CreateServiceAccountPayload();
                            serviceAccountPayload.name = name;
                            return api.createServiceAccount(serviceAccountPayload);
                        }));
    }
}
