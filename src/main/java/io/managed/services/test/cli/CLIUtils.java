package io.managed.services.test.cli;

import com.openshift.cloud.api.kas.auth.models.ConsumerGroup;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.ServiceAccountListItem;
import io.fabric8.kubernetes.api.model.AuthInfo;
import io.fabric8.kubernetes.api.model.Cluster;
import io.fabric8.kubernetes.api.model.Config;
import io.fabric8.kubernetes.api.model.Context;
import io.fabric8.kubernetes.api.model.NamedAuthInfo;
import io.fabric8.kubernetes.api.model.NamedCluster;
import io.fabric8.kubernetes.api.model.NamedContext;
import io.managed.services.test.Environment;
import io.managed.services.test.TestUtils;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaNotDeletedException;
import io.managed.services.test.client.kafkamgmt.KafkaNotReadyException;
import io.managed.services.test.client.kafkamgmt.KafkaUnknownHostsException;
import io.managed.services.test.client.oauth.KeycloakLoginSession;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.managed.services.test.TestUtils.bwait;
import static java.time.Duration.ofMinutes;

public class CLIUtils {
    private static final Logger LOGGER = LogManager.getLogger(CLIUtils.class);

    public static void extractCLI(InputStream archive, String entryMatch, Path dest) throws IOException {

        var bi = new BufferedInputStream(archive);
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

            if (e.isDirectory()) {
                throw new IOException("the entry " + e.getName() + " is a directory");
            } else {
                var parent = dest.getParent().toFile();
                if (!parent.isDirectory() && !parent.mkdirs()) {
                    throw new IOException("failed to create directory " + parent);
                }
                Files.copy(tar, dest);
                return;
            }
        }
        throw new IOException("cli not found");
    }

    public static void login(Vertx vertx, CLI cli, String username, String password) {
        var session = new KeycloakLoginSession(vertx, username, password);
        login(cli, session);
    }

    @SneakyThrows
    public static void login(CLI cli, KeycloakLoginSession session) {

        var authURL = String.format("%s/auth/realms/%s", Environment.REDHAT_SSO_URI, Environment.REDHAT_SSO_REALM);
        var masAuthURL = String.format("%s/auth/realms/%s", Environment.OPENSHIFT_IDENTITY_URI, Environment.OPENSHIFT_IDENTITY_REALM);

        LOGGER.info("start CLI login with username: {}", session.getUsername());
        var process = cli.login(Environment.OPENSHIFT_API_URI, authURL, masAuthURL);

        LOGGER.info("start oauth login against CLI");
        var redhatSSOURL = parseUrl(process.stdout(), String.format("%s/auth/.*", Environment.REDHAT_SSO_URI));
        bwait(session.login(redhatSSOURL));
        LOGGER.info("login to redhat sso completed");

        var masSSOURL = parseUrl(process.stdout(), String.format("%s/auth/.*", Environment.OPENSHIFT_IDENTITY_URI));
        bwait(session.login(masSSOURL));
        LOGGER.info("login to mas sso completed");

        process.future(ofMinutes(3)).get();
        LOGGER.info("CLI login completed");
    }

    @SneakyThrows
    private static String parseUrl(BufferedReader stdout, String urlRegex) {

        var full = new ArrayList<String>();
        while (true) {
            var l = stdout.readLine();
            if (l == null) {
                throw new Exception("SSO URL Not found\n-- STDOUT --\n" + String.join("\n", full));
            }

            if (l.matches(urlRegex)) {
                return l;
            }
            full.add(l);
        }
    }

    public static Optional<ConsumerGroup> getConsumerGroupByName(CLI cli, String consumerName) throws CliGenericException {
        try {
            return Optional.of(cli.describeConsumerGroup(consumerName));
        } catch (CliNotFoundException e) {
            return Optional.empty();
        }
    }

    public static KafkaRequest waitUntilKafkaIsReady(CLI cli, String id)
        throws KafkaUnknownHostsException, KafkaNotReadyException, InterruptedException, CliGenericException {

        return KafkaMgmtApiUtils.waitUntilKafkaIsReady(() -> cli.describeKafka(id));
    }

    public static void waitUntilKafkaIsDeleted(CLI cli, String id)
        throws KafkaNotDeletedException, InterruptedException, CliGenericException {

        KafkaMgmtApiUtils.waitUntilKafkaIsDeleted(() -> {
            try {
                return Optional.of(cli.describeKafka(id));
            } catch (CliNotFoundException e) {
                return Optional.empty();
            }
        });
    }

    public static Optional<ServiceAccountListItem> getServiceAccountByName(CLI cli, String name) throws CliGenericException {
        return cli.listServiceAccount().getItems().stream().filter(sa -> name.equals(sa.getName())).findAny();
    }

    public static ServiceAccountSecret createServiceAccount(CLI cli, String name) throws CliGenericException {
        var secretPath = Paths.get(cli.getWorkdir(), name + ".json");
        cli.createServiceAccount(name, secretPath);
        return getServiceAccountSecret(secretPath);
    }

    @SneakyThrows
    public static ServiceAccountSecret getServiceAccountSecret(Path secretPath) {
        return TestUtils.asJson(ServiceAccountSecret.class, Files.readString(secretPath));
    }

    @SneakyThrows
    public static ConsumerGroup waitForConsumerGroup(CLI cli, String name) {
        return KafkaInstanceApiUtils.waitForConsumerGroup(() -> getConsumerGroupByName(cli, name));
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
