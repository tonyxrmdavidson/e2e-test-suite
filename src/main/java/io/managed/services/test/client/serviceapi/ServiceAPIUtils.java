package io.managed.services.test.client.serviceapi;


import io.managed.services.test.DNSUtils;
import io.managed.services.test.Environment;
import io.managed.services.test.IsReady;
import io.managed.services.test.client.exception.HTTPToManyRequestsException;
import io.managed.services.test.client.exception.ResponseException;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.ext.auth.User;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.managed.services.test.TestUtils.message;
import static io.managed.services.test.TestUtils.waitFor;
import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;


@Deprecated
public class ServiceAPIUtils {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPIUtils.class);

    public static Future<ServiceAPI> serviceAPI(Vertx vertx) {
        return serviceAPI(vertx, Environment.SSO_USERNAME, Environment.SSO_PASSWORD);
    }

    public static Future<ServiceAPI> serviceAPI(Vertx vertx, String username, String password) {
        var auth = new KeycloakOAuth(vertx, username, password);

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        return auth.login(
                Environment.SSO_REDHAT_KEYCLOAK_URI,
                Environment.SSO_REDHAT_REDIRECT_URI,
                Environment.SSO_REDHAT_REALM,
                Environment.SSO_REDHAT_CLIENT_ID)

            .map(u -> serviceAPI(vertx, u));
    }

    public static ServiceAPI serviceAPI(Vertx vertx, User user) {
        return new ServiceAPI(vertx, Environment.SERVICE_API_URI, user);
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

        CreateKafkaPayload payload = createKafkaPayload(name);
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
                return api.createKafka(payload, true);
            }))
            .compose(k -> {

                if ("accepted".equals(k.status) || "provisioning".equals(k.status)) {
                    return waitUntilKafkaIsReady(vertx, api, k.id);
                }

                if ("ready".equals(k.status)) {
                    return succeededFuture(k);
                }
                return failedFuture(message("kafka instance status should be 'ready' but it is '{}'", k.status));
            })
            .onSuccess(k -> LOGGER.info("apply kafka instance: {}", Json.encode(k)));
    }

    /**
     * Create a Kafka instance but retry for 10 minutes if the cluster capacity is exhausted.
     *
     * @param vertx   Vertx
     * @param api     ServiceAPI
     * @param payload CreateKafkaPayload
     * @return Future<KafkaResponse>
     */
    public static Future<KafkaResponse> createKafkaInstance(Vertx vertx, ServiceAPI api, CreateKafkaPayload payload) {
        IsReady<KafkaResponse> isReady = last -> api.createKafka(payload, true)
            .map(k -> Pair.with(true, k))
            .recover(t -> {
                if (t instanceof HTTPToManyRequestsException) {
                    return Future.succeededFuture(Pair.with(false, null));
                }
                return Future.failedFuture(t);
            });

        return waitFor(vertx, "kafka instance to create", ofSeconds(10), ofMinutes(10), isReady);
    }

    public static CreateKafkaPayload createKafkaPayload(String kafkaInstanceName) {
        var kafkaPayload = new CreateKafkaPayload();
        kafkaPayload.name = kafkaInstanceName;
        kafkaPayload.multiAZ = true;
        kafkaPayload.cloudProvider = "aws";
        kafkaPayload.region = "us-east-1";
        return kafkaPayload;
    }

    /**
     * Delete the Kafka Instance if it exists and if the SKIP_KAFKA_TEARDOWN env is set to false.
     *
     * @param api  ServiceAPI
     * @param name Kafka Instance name
     * @return Future<Void>
     */
    public static Future<Void> cleanKafkaInstance(ServiceAPI api, String name) {
        if (Environment.SKIP_KAFKA_TEARDOWN) {
            LOGGER.warn("skip kafka instance clean up");
            return Future.succeededFuture();
        }

        return deleteKafkaByNameIfExists(api, name);
    }

    /**
     * Delete Kafka Instance by name if it exists
     *
     * @param api  ServiceAPI
     * @param name Kafka Instance name
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

    public static Future<Void> cleanServiceAccount(ServiceAPI api, String name) {
        return deleteServiceAccountByNameIfExists(api, name);
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

        return waitFor(vertx, "kafka instance to be ready", ofSeconds(10), ofMinutes(10), isReady)
            .compose(k -> waitUntilHostsAreResolved(vertx, k));
    }

    public static Future<KafkaResponse> waitUntilHostsAreResolved(Vertx vertx, KafkaResponse kafka) {
        var bootstrap = kafka.bootstrapServerHost.replaceFirst(":443$", "");
        var broker0 = "broker-0-" + bootstrap;
        var broker1 = "broker-1-" + bootstrap;
        var broker2 = "broker-2-" + bootstrap;
        var admin = "admin-server-" + bootstrap;

        var hosts = new ArrayList<>(List.of(bootstrap, admin, broker0, broker1, broker2));

        IsReady<Void> isDNSReady = last -> {

            for (var i = 0; i < hosts.size(); i++) {
                try {
                    var r = InetAddress.getByName(hosts.get(i));
                    LOGGER.info("host {} resolved: {}", hosts.get(i), r.getHostAddress());

                    // remove resolved hosts from the list
                    hosts.remove(i);
                    i--; // shift i back to not skip a host
                } catch (UnknownHostException e) {
                    LOGGER.debug("failed to resolve host {}: ", hosts.get(i), e);

                    LOGGER.debug("dig {}:\n{}", hosts.get(i), DNSUtils.dig(hosts.get(i)));
                    LOGGER.debug("dig {} 1.1.1.1:\n{}", hosts.get(i), DNSUtils.dig(hosts.get(i), "1.1.1.1"));

                    // print also the DNS lookup result if the host is still unavailable
                    if (last) {
                        LOGGER.warn("DNSLookup {}:\n{}", hosts.get(i), DNSUtils.dnsInfo(hosts.get(i)));
                    }
                }
            }
            return Future.succeededFuture(new Pair<>(hosts.isEmpty(), null));
        };

        return waitFor(vertx, "kafka hosts to be resolved", ofSeconds(5), ofMinutes(5), isDNSReady)
            .map(__ -> kafka);
    }

    public static Future<Pair<Boolean, KafkaResponse>> isKafkaReady(KafkaResponse kafka, boolean last) {
        LOGGER.debug("kafka instance status is: {}", kafka.status);

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

        return waitFor(vertx, "kafka instance to be deleted", ofSeconds(10), ofMinutes(10), isDeleted);
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
                    LOGGER.info("create service account: {}", name);

                    var serviceAccountPayload = new CreateServiceAccountPayload();
                    serviceAccountPayload.name = name;
                    return api.createServiceAccount(serviceAccountPayload);
                }));
    }
}
