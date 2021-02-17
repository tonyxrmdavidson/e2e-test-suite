package io.managed.services.test.client.serviceapi;


import io.managed.services.test.Environment;
import io.managed.services.test.IsReady;
import io.managed.services.test.client.ResponseException;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import java.util.Optional;

import static io.managed.services.test.TestUtils.await;
import static io.managed.services.test.TestUtils.waitFor;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;


public class ServiceAPIUtils {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAPIUtils.class);

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

        return getServiceAccountByName(api, name)
                .compose(o -> o.map(s -> {
                    LOGGER.info("clean service account: {}", s.id);
                    return api.deleteServiceAccount(s.id);
                }).orElseGet(() -> {
                    LOGGER.warn("service account '{}' not found", name);
                    return Future.succeededFuture();
                }));
    }

    /**
     * Function that returns kafkaResponse only if status is in ready
     *
     * @param vertx   Vertx
     * @param api     ServiceAPI
     * @param kafkaID String
     * @return KafkaResponse
     */
    public static KafkaResponse waitUntilKafkaIsReady(Vertx vertx, ServiceAPI api, String kafkaID) {
        KafkaResponse kafkaResponse;
        IsReady<KafkaResponse> isReady = last -> api.getKafka(kafkaID).map(r -> {
            LOGGER.info("kafka instance status is: {}", r.status);

            if (last) {
                LOGGER.warn("last kafka response is: {}", Json.encode(r));
            }
            return Pair.with(r.status.equals("ready"), r);
        });

        kafkaResponse = await(waitFor(vertx, "kafka instance to be ready", ofSeconds(10), ofMillis(Environment.WAIT_READY_MS), isReady));
        return kafkaResponse;
    }

    public static void waitUntilKafkaIsDelete(Vertx vertx, ServiceAPI api, String kafkaID) {
        await(api.deleteKafka(kafkaID, true));

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

        await(waitFor(vertx, "kafka instance to be deleted", ofSeconds(10), ofMillis(Environment.WAIT_READY_MS), isDeleted));
    }
}


