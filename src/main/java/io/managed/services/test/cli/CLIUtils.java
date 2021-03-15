package io.managed.services.test.cli;

import io.managed.services.test.Environment;
import io.managed.services.test.IsReady;
import io.managed.services.test.client.BaseVertxClient;
import io.managed.services.test.client.oauth.KeycloakOAuthUtils;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAccount;
import io.managed.services.test.client.serviceapi.ServiceAccountSecret;
import io.managed.services.test.client.serviceapi.TopicResponse;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientSession;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Optional;

import static io.managed.services.test.TestUtils.message;
import static io.managed.services.test.TestUtils.waitFor;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;

public class CLIUtils {
    private static final Logger LOGGER = LogManager.getLogger(CLIUtils.class);

    public static void extractCLI(String archive, String entry, String dest) throws IOException {

        var bi = new BufferedInputStream(Files.newInputStream(Paths.get(archive)));
        var gzi = new GzipCompressorInputStream(bi);
        var tar = new TarArchiveInputStream(gzi);

        ArchiveEntry e;
        while ((e = tar.getNextEntry()) != null) {
            if (!e.getName().equals(entry)) {
                continue;
            }

            LOGGER.info("extract {} to {}", e.getName(), dest);

            if (!tar.canReadEntryData(e)) {
                throw new IOException("can not read entry " + e.getName());
            }

            File f = new File(dest);
            if (e.isDirectory()) {
                throw new IOException("the entry " + e.getName() + " is a directory");
            } else {
                File parent = f.getParentFile();
                if (!parent.isDirectory() && !parent.mkdirs()) {
                    throw new IOException("failed to create directory " + parent);
                }
                try (OutputStream o = Files.newOutputStream(f.toPath())) {
                    IOUtils.copy(tar, o);
                    return;
                }
            }
        }
        throw new IOException("cli not found");
    }

    public static Future<Void> login(Vertx vertx, CLI cli, String username, String password) {
        WebClient client = WebClient.create(vertx);
        WebClientSession session = WebClientSession.create(client);

        LOGGER.info("start CLI login with username: {}", username);
        return cli.login()
                .compose(process -> {

                    LOGGER.info("start oauth login against CLI");
                    var oauthFuture = parseUrl(vertx, process.stdout(), "https://sso.redhat.com/auth/.*")
                            .compose(l -> KeycloakOAuthUtils.startLogin(session, l))
                            .compose(r -> KeycloakOAuthUtils.postUsernamePassword(session, r, username, password))
                            .compose(r -> BaseVertxClient.assertResponse(r, HttpURLConnection.HTTP_MOVED_TEMP))
                            .compose(r -> BaseVertxClient.followRedirect(session, r))
                            .compose(r -> BaseVertxClient.assertResponse(r, HttpURLConnection.HTTP_OK))
                            .map(v -> {
                                LOGGER.info("first oauth login completed");
                                return null;
                            });

                    var edgeSSOFuture = parseUrl(vertx, process.stdout(), "https://keycloak-edge-redhat-rhoam-user-sso.apps.mas-sso-stage.1gzl.s1.devshift.org.*")
                            .compose(l -> KeycloakOAuthUtils.startLogin(session, l))
                            .compose(r -> KeycloakOAuthUtils.postUsernamePassword(session, r, username, password))
                            .compose(r -> BaseVertxClient.assertResponse(r, HttpURLConnection.HTTP_MOVED_TEMP))
                            .compose(r -> BaseVertxClient.followRedirect(session, r))
                            .compose(r -> BaseVertxClient.assertResponse(r, HttpURLConnection.HTTP_OK))
                            .map(v -> {
                                LOGGER.info("second oauth login completed");
                                return null;
                            });

                    var cliFuture = process.future()
                            .map(r -> {
                                LOGGER.info("CLI login completed");
                                return null;
                            });

                    return CompositeFuture.all(oauthFuture, edgeSSOFuture, cliFuture);
                })
                .map(n -> null);
    }

    private static Future<String> parseUrl(Vertx vertx, BufferedReader stdout, String urlRegex) {

        return vertx.executeBlocking(h -> {
            var full = new ArrayList<String>();
            while (true) {
                try {
                    var l = stdout.readLine();
                    if (l == null) {
                        h.fail(new Exception("SSO URL Not found\n-- STDOUT --\n" + String.join("\n", full)));
                        return;
                    }

                    if (l.matches(urlRegex)) {
                        h.complete(l);
                        return;
                    }
                    full.add(l);

                } catch (Exception e) {
                    h.fail(e);
                    return;
                }
            }
        });
    }

    public static Future<?> deleteKafkaByNameIfExists(CLI cli, String name) {
        return getKafkaByName(cli, name)
                .compose(o -> o.map(k -> {
                    LOGGER.info("delete kafka instance: {}", k.id);
                    return cli.deleteKafka(k.id);
                }).orElseGet(() -> {
                    LOGGER.warn("kafka instance '{}' not found", name);
                    return Future.succeededFuture();
                }));
    }

    public static Future<Optional<TopicResponse>> getTopicByName(CLI cli, String topicName) {
        return cli.listTopics().map(r -> r.items != null ? r.items.stream().filter(topic -> topic.name.equals(topicName)).findFirst() : Optional.empty());
    }

    public static Future<Optional<KafkaResponse>> getKafkaByName(CLI cli, String name) {
        return cli.listKafkaByNameAsJson(name)
                .map(r -> r.items != null ? r.items.stream().findFirst() : Optional.empty());
    }

    public static Future<KafkaResponse> waitForKafkaReady(Vertx vertx, CLI cli, String id) {
        IsReady<KafkaResponse> isReady = last -> cli.describeKafka(id).map(r -> {
            LOGGER.info("kafka instance status is: {}", r.status);

            if (last) {
                LOGGER.warn("last kafka response is: {}", Json.encode(r));
            }
            return Pair.with(r.status.equals("ready"), r);
        });

        return waitFor(vertx, "kafka instance to be ready", ofSeconds(10), ofMillis(Environment.WAIT_READY_MS), isReady);
    }

    public static Future<Void> waitForKafkaDelete(Vertx vertx, CLI cli, String name) {
        IsReady<Void> isDeleted = last -> getKafkaByName(cli, name)
                .map(k -> Pair.with(k.isEmpty(), null));

        return waitFor(vertx, "kafka instance to be deleted", ofSeconds(10), ofMillis(Environment.WAIT_READY_MS), isDeleted);
    }

    public static Future<Optional<ServiceAccount>> getServiceAccountByName(CLI cli, String name) {
        return cli.listServiceAccountAsJson()
                .map(r -> r.items.stream().filter(sa -> sa.name.equals(name)).findFirst());
    }

    public static Future<?> deleteServiceAccountByNameIfExists(CLI cli, String name) {
        return getServiceAccountByName(cli, name)
                .compose(o -> o.map(k -> {
                    LOGGER.info("delete serviceaccount {} instance: {}", k.name, k.id);
                    return cli.deleteServiceAccount(k.id);
                }).orElseGet(() -> {
                    LOGGER.warn("serviceaccount '{}' not found", name);
                    return Future.succeededFuture();
                }));
    }

    public static Future<ServiceAccount> createServiceAccount(CLI cli, String name) {
        return cli.createServiceAccount(name, Paths.get("/tmp", name + ".json"))
                .compose(p -> getServiceAccountByName(cli, name))
                .compose(o -> o
                        .map(Future::succeededFuture)
                        .orElseGet(() -> Future.failedFuture(message("failed to find created service account: {}", name))));
    }

    public static ServiceAccountSecret getServiceAccountSecret(String secretName) throws IOException {
        return ProcessUtils.asJson(ServiceAccountSecret.class, Files.readString(Paths.get("/tmp", secretName + ".json")));
    }

    public static Future<Void> waitForTopicDelete(Vertx vertx, CLI cli, String topicName) {
        IsReady<Void> isDeleted = last -> getTopicByName(cli, topicName)
                .map(k -> Pair.with(k.isEmpty(), null));
        return waitFor(vertx, "kafka topic to be deleted", ofSeconds(10), ofSeconds(Environment.WAIT_READY_MS), isDeleted);
    }

}
