package io.managed.services.test.cli;

import io.fabric8.kubernetes.api.model.AuthInfo;
import io.fabric8.kubernetes.api.model.Cluster;
import io.fabric8.kubernetes.api.model.Config;
import io.fabric8.kubernetes.api.model.Context;
import io.fabric8.kubernetes.api.model.NamedAuthInfo;
import io.fabric8.kubernetes.api.model.NamedCluster;
import io.fabric8.kubernetes.api.model.NamedContext;
import io.managed.services.test.Environment;
import io.managed.services.test.IsReady;
import io.managed.services.test.TestUtils;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.managed.services.test.client.serviceapi.ConsumerGroupResponse;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.client.serviceapi.ServiceAccount;
import io.managed.services.test.client.serviceapi.ServiceAccountSecret;
import io.managed.services.test.client.serviceapi.TopicResponse;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.managed.services.test.TestUtils.message;
import static io.managed.services.test.TestUtils.waitFor;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;

public class CLIUtils {
    private static final Logger LOGGER = LogManager.getLogger(CLIUtils.class);

    public static void extractCLI(String archive, String entryMatch, String dest) throws IOException {

        var bi = new BufferedInputStream(Files.newInputStream(Paths.get(archive)));
        var gzi = new GzipCompressorInputStream(bi);
        var tar = new TarArchiveInputStream(gzi);

        ArchiveEntry e;
        while ((e = tar.getNextEntry()) != null) {
            if (!e.getName().matches(entryMatch)) {
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

        var authURL = String.format("%s/auth/realms/%s", Environment.SSO_REDHAT_KEYCLOAK_URI, Environment.SSO_REDHAT_REALM);
        var masAuthURL = String.format("%s/auth/realms/%s", Environment.MAS_SSO_REDHAT_KEYCLOAK_URI, Environment.MAS_SSO_REDHAT_REALM);

        LOGGER.info("start CLI login with username: {}", username);
        return cli.login(Environment.SERVICE_API_URI, authURL, masAuthURL)
                .compose(process -> {

                    var oauth2 = new KeycloakOAuth(vertx);

                    LOGGER.info("start oauth login against CLI");
                    var oauthFuture = parseUrl(vertx, process.stdout(), String.format("%s/auth/.*", Environment.SSO_REDHAT_KEYCLOAK_URI))
                            .compose(l -> oauth2.login(l, username, password))
                            .onSuccess(__ -> LOGGER.info("first oauth login completed"));

                    var edgeSSOFuture = parseUrl(vertx, process.stdout(), String.format("%s/auth/.*", Environment.MAS_SSO_REDHAT_KEYCLOAK_URI))
                            .compose(l -> oauth2.login(l, username, password))
                            .onSuccess(__ -> LOGGER.info("second oauth login completed without username and password"));

                    var cliFuture = process.future(ofMinutes(3))
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

    public static Future<?> deleteKafkaByNameIfExists(Vertx vertx, CLI cli, String name) {
        return getKafkaByName(vertx, cli, name)
                .compose(o -> o.map(k -> {
                    LOGGER.info("delete kafka instance: {}", k.id);
                    return cli.deleteKafka(k.id);
                }).orElseGet(() -> {
                    LOGGER.warn("kafka instance '{}' not found", name);
                    return Future.succeededFuture();
                }));
    }

    public static Future<Optional<TopicResponse>> getTopicByName(Vertx vertx, CLI cli, String topicName) {
        return cli.listTopics().map(r -> r.items != null ? r.items.stream().filter(topic -> topic.name.equals(topicName)).findFirst() : Optional.empty());
    }

    public static Future<Optional<ConsumerGroupResponse>> getConsumerGroupByName(CLI cli, String topicName) {
        return cli.listConsumerGroups().map(r -> r.items != null ? r.items.stream().filter(consumerGroup -> consumerGroup.groupId.equals(topicName)).findFirst() : Optional.empty());
    }

    public static Future<Optional<KafkaResponse>> getKafkaByName(Vertx vertx, CLI cli, String name) {
        return cli.listKafkaByNameAsJson(name)
                .map(r -> r.items != null ? r.items.stream().findFirst() : Optional.empty());
    }

    public static Future<KafkaResponse> waitForKafkaReady(Vertx vertx, CLI cli, String id) {
        IsReady<KafkaResponse> isReady = last -> cli.describeKafka(id)
                .compose(r -> ServiceAPIUtils.isKafkaReady(r, last));
        return waitFor(vertx, "kafka instance to be ready", ofSeconds(10), ofMillis(Environment.WAIT_READY_MS), isReady);
    }

    public static Future<Void> waitForKafkaDelete(Vertx vertx, CLI cli, String name) {
        IsReady<Void> isDeleted = last -> getKafkaByName(vertx, cli, name)
                .map(k -> Pair.with(k.isEmpty(), null));

        return waitFor(vertx, "kafka instance to be deleted", ofSeconds(10), ofMillis(Environment.WAIT_READY_MS), isDeleted);
    }

    public static Future<Optional<ServiceAccount>> getServiceAccountByName(Vertx vertx, CLI cli, String name) {
        return cli.listServiceAccountAsJson()
                .map(r -> r.items.stream().filter(sa -> sa.name.equals(name)).findFirst());
    }

    public static Future<?> deleteServiceAccountByNameIfExists(Vertx vertx, CLI cli, String name) {
        return getServiceAccountByName(vertx, cli, name)
                .compose(o -> o.map(k -> {
                    LOGGER.info("delete serviceaccount {} instance: {}", k.name, k.id);
                    return cli.deleteServiceAccount(k.id);
                }).orElseGet(() -> {
                    LOGGER.warn("serviceaccount '{}' not found", name);
                    return Future.succeededFuture();
                }));
    }

    public static Future<ServiceAccount> createServiceAccount(Vertx vertx, CLI cli, String name) {
        return cli.createServiceAccount(name, Paths.get(cli.getWorkdir(), name + ".json"))
                .compose(p -> getServiceAccountByName(vertx, cli, name))
                .compose(o -> o
                        .map(Future::succeededFuture)
                        .orElseGet(() -> Future.failedFuture(message("failed to find created service account: {}", name))));
    }

    public static ServiceAccountSecret getServiceAccountSecret(CLI cli, String secretName) throws IOException {
        return TestUtils.asJson(ServiceAccountSecret.class, Files.readString(Paths.get(cli.getWorkdir(), secretName + ".json")));
    }

    public static Future<Void> waitForTopicDelete(Vertx vertx, CLI cli, String topicName) {
        IsReady<Void> isDeleted = last -> getTopicByName(vertx, cli, topicName)
                .map(k -> Pair.with(k.isEmpty(), null));
        return waitFor(vertx, "kafka topic to be deleted", ofSeconds(10), ofSeconds(Environment.WAIT_READY_MS), isDeleted);
    }

    public static Config kubeConfig(String server, String token, String namespace) {
        var cluster = new Cluster();
        cluster.setServer(server);

        var namedCluster = new NamedCluster();
        namedCluster.setCluster(cluster);
        namedCluster.setName("default");

        var authInfo = new AuthInfo();
        authInfo.setToken(token);

        var namedAuthInfo = new NamedAuthInfo();
        namedAuthInfo.setUser(authInfo);
        namedAuthInfo.setName("default");

        var context = new Context();
        context.setCluster("default");
        context.setUser("default");
        context.setNamespace(namespace);

        var namedContext = new NamedContext();
        namedContext.setContext(context);
        namedContext.setName("default");

        var c = new Config();
        c.setApiVersion("v1");
        c.setKind("Config");
        c.setClusters(List.of(namedCluster));
        c.setUsers(List.of(namedAuthInfo));
        c.setContexts(List.of(namedContext));
        c.setCurrentContext("default");

        return c;
    }
}
