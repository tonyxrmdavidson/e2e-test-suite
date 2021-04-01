package io.managed.services.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.managed.services.test.cli.CLI;
import io.managed.services.test.cli.CLIDownloader;
import io.managed.services.test.cli.CLIUtils;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.client.serviceapi.ServiceAPIUtils;
import io.managed.services.test.framework.TestTag;
import io.managed.services.test.operator.OperatorUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.auth.User;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.TimeUnit;

import static io.managed.services.test.TestUtils.decodeBase64;
import static io.managed.services.test.TestUtils.message;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(TestTag.BINDING_OPERATOR)
@ExtendWith(VertxExtension.class)
@Timeout(value = 5, timeUnit = TimeUnit.MINUTES)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class QuarkusSampleTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(CLITest.class);

    // use the kafka long living instance
    static final String KAFKA_INSTANCE_NAME = "mk-e2e-ll-" + Environment.KAFKA_POSTFIX_NAME;

    // this name is decided from the cli
    static final String ACCESS_TOKEN_SECRET_NAME = "rh-cloud-services-accesstoken-cli";
    static final String SERVICE_ACCOUNT_SECRET_NAME = "rh-cloud-services-service-account";

    private void assertENVs() {
        assumeTrue(Environment.SSO_USERNAME != null, "the SSO_USERNAME env is null");
        assumeTrue(Environment.SSO_PASSWORD != null, "the SSO_PASSWORD env is null");
        assumeTrue(Environment.DEV_CLUSTER_TOKEN != null, "the DEV_CLUSTER_TOKEN env is null");
        assumeTrue(Environment.BF2_GITHUB_TOKEN != null, "the BF2_GITHUB_TOKEN env is null");
    }

    CLI cli;
    User user;
    ServiceAPI api;
    KubernetesClient client;

    private void bootstrapK8sClient() {

        var config = new ConfigBuilder()
                .withMasterUrl(Environment.DEV_CLUSTER_SERVER)
                .withOauthToken(Environment.DEV_CLUSTER_TOKEN)
                .withNamespace(Environment.DEV_CLUSTER_NAMESPACE)
                .build();

        LOGGER.info("initialize kubernetes client");
        client = new DefaultKubernetesClient(config);
    }

    private Future<Void> bootstrapCLI(Vertx vertx) {

        var downloader = CLIDownloader.defaultDownloader(vertx);
        return downloader.downloadCLIInTempDir()
                .compose(binary -> {
                    LOGGER.info("cli downloaded successfully to: {}", binary.directory);
                    this.cli = new CLI(vertx, binary.directory, binary.name);

                    return CLIUtils.login(vertx, cli, Environment.SSO_USERNAME, Environment.SSO_PASSWORD);
                });
    }

    private Future<Void> bootstrapUserAndAPI(Vertx vertx) {

        KeycloakOAuth auth = new KeycloakOAuth(vertx,
                Environment.SSO_REDHAT_KEYCLOAK_URI,
                Environment.SSO_REDHAT_REDIRECT_URI,
                Environment.SSO_REDHAT_REALM,
                Environment.SSO_REDHAT_CLIENT_ID);

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        return auth.login(Environment.SSO_USERNAME, Environment.SSO_PASSWORD)
                .map(u -> {
                    user = u;

                    api = new ServiceAPI(vertx, Environment.SERVICE_API_URI, user);

                    return null;
                });
    }

    @BeforeAll
    void bootstrap(Vertx vertx, VertxTestContext context) {
        assertENVs();

        bootstrapK8sClient();

        bootstrapCLI(vertx)

                .compose(__ -> bootstrapUserAndAPI(vertx))

                .onComplete(context.succeedingThenComplete());
    }

    private void cleanAccessTokenSecret() {
        Secret s = client.secrets().withName(ACCESS_TOKEN_SECRET_NAME).get();
        if (s != null) {
            LOGGER.info("clean secret: {}", s.getMetadata().getName());
            client.secrets().delete(s);
        }
    }

    private void cleanKafkaConnection() {
        var c = OperatorUtils.kafkaConnection(client).withName(KAFKA_INSTANCE_NAME).get();
        if (c != null) {
            LOGGER.info("clean ManagedKafkaConnection: {}", c.getMetadata().getName());
            OperatorUtils.kafkaConnection(client).delete(c);
        }
    }

    private Future<Void> cleanServiceAccount() {
        var secret = client.secrets().withName(SERVICE_ACCOUNT_SECRET_NAME).get();
        if (secret == null) {
            return Future.failedFuture(message("failed to find secret with name: {}", SERVICE_ACCOUNT_SECRET_NAME));
        }

        LOGGER.info("clean service account secret: {}", secret.getMetadata().getName());
        var encodedClientID = secret.getData().get("client-id");
        if (encodedClientID == null) {
            return Future.failedFuture(message("client-id data not found in secret: {}", secret));
        }
        var secretClientID = decodeBase64(encodedClientID);

        return api.getListOfServiceAccounts()
                // find the service account with the same client-id as the secret
                .map(accounts -> accounts.items.stream()
                        .filter(a -> a.clientID.equals(secretClientID))
                        .findAny())

                // unwrap the optional service account
                .compose(o -> o
                        .map(a -> Future.succeededFuture(a))
                        .orElse(Future.failedFuture(message("failed to find service account with client-id: {}", secretClientID))))

                // delete the service account if founded
                .compose(a -> {
                    LOGGER.info("delete service account {} with id: {}", a.name, a.id);
                    return api.deleteServiceAccount(a.id);
                })

                // delete the secret only after deleting the service account
                .map(__ -> {
                    LOGGER.info("delete secret: {}", secret.getMetadata().getName());
                    client.secrets().delete(secret);

                    return null;
                });

    }

    private Future<Void> cleanCLI(Vertx vertx) {
        if (cli != null) {
            LOGGER.info("logout from cli");
            return cli.logout()

                    .compose(__ -> {
                        LOGGER.info("clean workdir: {}", cli.getWorkdir());
                        return vertx.fileSystem().deleteRecursive(cli.getWorkdir(), true);
                    });
        }
        return Future.succeededFuture();
    }

    @AfterAll
    void teardown(Vertx vertx, VertxTestContext context) {
        try {
            cleanAccessTokenSecret();
        } catch (Exception e) {
            LOGGER.error("cleanAccessTokenSecret error: ", e);
        }

        try {
            cleanKafkaConnection();
        } catch (Exception e) {
            LOGGER.error("cleanKafkaConnection error: ", e);
        }

        cleanServiceAccount()
                .recover(e -> {
                    LOGGER.error("cleanServiceAccount error: ", e);
                    return Future.succeededFuture();
                })

                .compose(__ -> cleanCLI(vertx))
                .recover(e -> {
                    LOGGER.error("cleanCLI error: ", e);
                    return Future.succeededFuture();
                })

                .onComplete(context.succeedingThenComplete());
    }

    @Test
    void testCLIConnectCluster(Vertx vertx, VertxTestContext context) throws JsonProcessingException {
        var kubeConfig = CLIUtils.kubeConfig(
                Environment.DEV_CLUSTER_SERVER,
                Environment.DEV_CLUSTER_TOKEN,
                Environment.DEV_CLUSTER_NAMESPACE);

        var mapper = new ObjectMapper(new YAMLFactory());
        var config = mapper.writeValueAsBytes(kubeConfig);

        var kubeconfgipath = cli.getWorkdir() + "/kubeconfig";
        vertx.fileSystem().writeFile(kubeconfgipath, Buffer.buffer(config))

                .compose(__ -> {
                    LOGGER.info("retrieve long living kafka instance id");
                    return ServiceAPIUtils.getKafkaByName(api, KAFKA_INSTANCE_NAME).map(o -> o.orElseThrow());
                })

                .compose(kafka -> {
                    LOGGER.info("cli use kafka instance: {}", kafka.id);
                    return cli.useKafka(kafka.id);
                })

                .compose(__ -> {
                    LOGGER.info("kubeconfig create at: {}", kubeconfgipath);
                    return cli.connectCluster(KeycloakOAuth.getRefreshToken(user), kubeconfgipath);
                })

                .onComplete(context.succeedingThenComplete());
    }
}
