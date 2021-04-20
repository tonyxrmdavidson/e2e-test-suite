package io.managed.services.test;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.client.DefaultOpenShiftClient;
import io.fabric8.openshift.client.OpenShiftClient;
import io.managed.services.test.cli.CLI;
import io.managed.services.test.cli.CLIDownloader;
import io.managed.services.test.cli.CLIUtils;
import io.managed.services.test.client.oauth.KeycloakOAuth;
import io.managed.services.test.client.sample.QuarkusSample;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAPI;
import io.managed.services.test.framework.LogCollector;
import io.managed.services.test.framework.TestTag;
import io.managed.services.test.operator.OperatorUtils;
import io.managed.services.test.operator.ServiceBinding;
import io.managed.services.test.operator.ServiceBindingSpec;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import io.vertx.ext.auth.User;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static io.managed.services.test.TestUtils.decodeBase64;
import static io.managed.services.test.TestUtils.message;
import static io.managed.services.test.TestUtils.waitFor;
import static io.managed.services.test.client.kafkaadminapi.KafkaAdminAPIUtils.applyTopics;
import static io.managed.services.test.client.kafkaadminapi.KafkaAdminAPIUtils.kafkaAdminAPI;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.applyKafkaInstance;
import static io.managed.services.test.client.serviceapi.ServiceAPIUtils.deleteKafkaByNameIfExists;
import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(TestTag.BINDING_OPERATOR)
@ExtendWith(VertxExtension.class)
@Timeout(value = 5, timeUnit = TimeUnit.MINUTES)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class QuarkusSampleTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(QuarkusSampleTest.class);

    // NOTE: Some of the names are hard coded because generated from CLI or hard coded in the yaml files

    private static final String KAFKA_INSTANCE_NAME = "mk-e2e-quarkus-" + Environment.KAFKA_POSTFIX_NAME;
    private static final String TOPIC_NAME = "prices";

    // this name is decided from the cli
    private static final String ACCESS_TOKEN_SECRET_NAME = "rh-cloud-services-accesstoken-cli";
    private static final String SERVICE_ACCOUNT_SECRET_NAME = "rh-cloud-services-service-account";

    private static final String SERVICE_BINDING_NAME = "rhoas-kafka-quickstart-bind";

    private static final String APP_YAML_PATH = "quarkus/rhoas-kafka-quickstart-example.yml";
    private static final String APP_SERVICE_NAME = "rhoas-kafka-quickstart-example";
    private static final String APP_DEPLOYMENT_NAME = "rhoas-kafka-quickstart-example";
    private static final String APP_ROUTE_NAME = "rhoas-kafka-quickstart-example";

    private void assertENVs() {
        assumeTrue(Environment.SSO_USERNAME != null, "the SSO_USERNAME env is null");
        assumeTrue(Environment.SSO_PASSWORD != null, "the SSO_PASSWORD env is null");
        assumeTrue(Environment.DEV_CLUSTER_SERVER != null, "the DEV_CLUSTER_SERVER env is null");
        assumeTrue(Environment.DEV_CLUSTER_TOKEN != null, "the DEV_CLUSTER_TOKEN env is null");
        assumeTrue(Environment.BF2_GITHUB_TOKEN != null, "the BF2_GITHUB_TOKEN env is null");
    }


    Vertx vertx = Vertx.vertx();
    CLI cli;
    User user;
    ServiceAPI api;
    OpenShiftClient oc;
    KafkaResponse kafka;
    Route route;


    private void assertBootstrap() {
        assumeTrue(cli != null, "cli is null because the bootstrap has failed");
        assumeTrue(user != null, "user is null because the bootstrap has failed");
        assumeTrue(api != null, "api is null because the bootstrap has failed");
        assumeTrue(oc != null, "oc is null because the bootstrap has failed");
        assumeTrue(kafka != null, "kafka is null because the bootstrap has failed");
    }

    private void assertRoute() {
        assumeTrue(route != null, "route is null because the testDeployQuarkusSampleApp has failed");
    }

    private static InputStream getResource(String path) {
        return QuarkusSampleTest.class.getClassLoader().getResourceAsStream(path);
    }

    private List<HasMetadata> loadK8sResources(String path) {
        return oc.load(getResource(path)).get();
    }

    private void bootstrapK8sClient() {

        var config = new ConfigBuilder()
                .withMasterUrl(Environment.DEV_CLUSTER_SERVER)
                .withOauthToken(Environment.DEV_CLUSTER_TOKEN)
                .withNamespace(Environment.DEV_CLUSTER_NAMESPACE)
                .build();

        LOGGER.info("initialize kubernetes client");
        oc = new DefaultOpenShiftClient(config);
    }

    private Future<Void> bootstrapCLI() {

        var downloader = CLIDownloader.defaultDownloader(vertx);
        return downloader.downloadCLIInTempDir()
                .compose(binary -> {
                    LOGGER.info("cli downloaded successfully to: {}", binary.directory);
                    this.cli = new CLI(vertx, binary.directory, binary.name);

                    return CLIUtils.login(vertx, cli, Environment.SSO_USERNAME, Environment.SSO_PASSWORD);
                });
    }

    private Future<Void> bootstrapUserAndAPI() {

        KeycloakOAuth auth = new KeycloakOAuth(vertx);

        LOGGER.info("authenticate user: {} against: {}", Environment.SSO_USERNAME, Environment.SSO_REDHAT_KEYCLOAK_URI);
        return auth.login(
                Environment.SSO_REDHAT_KEYCLOAK_URI,
                Environment.SSO_REDHAT_REDIRECT_URI,
                Environment.SSO_REDHAT_REALM,
                Environment.SSO_REDHAT_CLIENT_ID,
                Environment.SSO_USERNAME,
                Environment.SSO_PASSWORD)

                .map(u -> {
                    user = u;

                    api = new ServiceAPI(vertx, Environment.SERVICE_API_URI, user);

                    return null;
                });
    }

    private Future<Void> bootstrapKafkaInstance() {
        return applyKafkaInstance(vertx, api, KAFKA_INSTANCE_NAME)
                .onSuccess(k -> kafka = k)

                .compose(__ -> kafkaAdminAPI(
                        vertx,
                        kafka.bootstrapServerHost,
                        Environment.SSO_USERNAME,
                        Environment.SSO_PASSWORD))
                .compose(admin -> applyTopics(admin, Set.of(TOPIC_NAME)))

                .map(__ -> null);
    }

    @Test
    @Order(0)
    @Timeout(value = 15, timeUnit = TimeUnit.MINUTES)
    void bootstrap(VertxTestContext context) {
        assertENVs();

        bootstrapK8sClient();

        bootstrapCLI()

                .compose(__ -> bootstrapUserAndAPI())

                .compose(__ -> bootstrapKafkaInstance())

                .onComplete(context.succeedingThenComplete());
    }

    private void collectQuarkusAppLogs(ExtensionContext context) throws IOException {
        LogCollector.saveDeploymentLog(
            TestUtils.getLogPath(Environment.LOG_DIR.resolve("test-logs").toString(), context),
            oc, Environment.DEV_CLUSTER_NAMESPACE, APP_DEPLOYMENT_NAME);
    }

    private void cleanAccessTokenSecret() {
        Secret s = oc.secrets().withName(ACCESS_TOKEN_SECRET_NAME).get();
        if (s != null) {
            LOGGER.info("clean secret: {}", s.getMetadata().getName());
            oc.secrets().delete(s);
        }
    }

    private void cleanKafkaConnection() {
        var c = OperatorUtils.kafkaConnection(oc).withName(KAFKA_INSTANCE_NAME).get();
        if (c != null) {
            LOGGER.info("clean ManagedKafkaConnection: {}", c.getMetadata().getName());
            OperatorUtils.kafkaConnection(oc).delete(c);
        }
    }

    private void cleanServiceBinding() {
        var b = OperatorUtils.serviceBinding(oc).withName(SERVICE_BINDING_NAME).get();
        if (b != null) {
            LOGGER.info("clean ServiceBinding: {}", b.getMetadata().getName());
            OperatorUtils.serviceBinding(oc).delete(b);
        }
    }

    private void cleanQuarkusSampleApp() {
        oc.apps().deployments().withName(APP_DEPLOYMENT_NAME).delete();
        oc.services().withName(APP_SERVICE_NAME).delete();
        oc.routes().withName(APP_ROUTE_NAME).delete();
    }

    private Future<Void> cleanServiceAccount() {
        var secret = oc.secrets().withName(SERVICE_ACCOUNT_SECRET_NAME).get();
        if (secret == null) {
            return failedFuture(message("failed to find secret with name: {}", SERVICE_ACCOUNT_SECRET_NAME));
        }

        LOGGER.info("clean service account secret: {}", secret.getMetadata().getName());
        var encodedClientID = secret.getData().get("client-id");
        if (encodedClientID == null) {
            return failedFuture(message("client-id data not found in secret: {}", secret));
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
                        .orElse(failedFuture(message("failed to find service account with client-id: {}", secretClientID))))

                // delete the service account if founded
                .compose(a -> {
                    LOGGER.info("delete service account {} with id: {}", a.name, a.id);
                    return api.deleteServiceAccount(a.id);
                })

                // delete the secret only after deleting the service account
                .map(__ -> {
                    LOGGER.info("delete secret: {}", secret.getMetadata().getName());
                    oc.secrets().delete(secret);

                    return null;
                });

    }

    private Future<Void> cleanKafkaInstance() {
        return deleteKafkaByNameIfExists(api, KAFKA_INSTANCE_NAME);
    }

    private Future<Void> cleanCLI() {
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
    void teardown(VertxTestContext context, ExtensionContext extensionContext) {
        assumeFalse(Environment.SKIP_TEARDOWN, "skip teardown");

        try {
            collectQuarkusAppLogs(extensionContext);
        } catch (Exception e) {
            LOGGER.error("collectQuarkusAppLogs error: ", e);
        }

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

        try {
            cleanServiceBinding();
        } catch (Exception e) {
            LOGGER.error("cleanServiceBinding error: ", e);
        }

        try {
            cleanQuarkusSampleApp();
        } catch (Exception e) {
            LOGGER.error("cleanQuarkusSampleApp error: ", e);
        }

        cleanServiceAccount()
                .recover(e -> {
                    LOGGER.error("cleanServiceAccount error: ", e);
                    return Future.succeededFuture();
                })

                .compose(__ -> cleanKafkaInstance())
                .recover(e -> {
                    LOGGER.error("cleanKafkaInstance error: ", e);
                    return Future.succeededFuture();
                })

                .compose(__ -> cleanCLI())
                .recover(e -> {
                    LOGGER.error("cleanCLI error: ", e);
                    return Future.succeededFuture();
                })

                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(1)
    void testCLIConnectCluster(VertxTestContext context) {
        assertBootstrap();

        cleanAccessTokenSecret();
        cleanKafkaConnection();

        var kubeConfig = CLIUtils.kubeConfig(
                Environment.DEV_CLUSTER_SERVER,
                Environment.DEV_CLUSTER_TOKEN,
                Environment.DEV_CLUSTER_NAMESPACE);

        var config = Serialization.asYaml(kubeConfig);

        var kubeconfgipath = cli.getWorkdir() + "/kubeconfig";
        vertx.fileSystem().writeFile(kubeconfgipath, Buffer.buffer(config))

                .compose(__ -> {
                    LOGGER.info("cli use kafka instance: {}", kafka.id);
                    return cli.useKafka(kafka.id);
                })

                .compose(__ -> {
                    LOGGER.info("cli cluster connect using kubeconfig: {}", kubeconfgipath);
                    return cli.connectCluster(KeycloakOAuth.getRefreshToken(user), kubeconfgipath);
                })

                .onComplete(context.succeedingThenComplete());
    }

    @Test
    @Order(2)
    void testDeployQuarkusSampleApp() {
        assertBootstrap();

        LOGGER.info("deploy the rhoas-kafka-quickstart-example app");
        oc.resourceList(loadK8sResources(APP_YAML_PATH)).createOrReplace();

        route = oc.routes().withName(APP_ROUTE_NAME).get();
        LOGGER.info("app deployed to: {}", route.getSpec().getHost());
    }


    @Test
    @Order(3)
    void testDeployServiceBinding() {
        assertBootstrap();

        cleanServiceBinding();

        var application = new ServiceBindingSpec.Application();
        application.group = "apps";
        application.name = APP_DEPLOYMENT_NAME;
        application.resource = "deployments";
        application.version = "v1";

        var service = new ServiceBindingSpec.Service();
        service.group = "rhoas.redhat.com";
        service.version = "v1alpha1";
        service.kind = "KafkaConnection";
        service.name = KAFKA_INSTANCE_NAME;

        var spec = new ServiceBindingSpec();
        spec.application = application;
        spec.bindAsFiles = true;
        spec.services = List.of(service);

        var binding = new ServiceBinding();
        binding.setMetadata(new ObjectMeta());
        binding.getMetadata().setName(SERVICE_BINDING_NAME);
        binding.setSpec(spec);

        LOGGER.info("deploy service binding");
        binding = OperatorUtils.serviceBinding(oc).createOrReplace(binding);
        LOGGER.info("service binding created: {}", binding);
    }

    @Test
    @Order(4)
    void testQuarkusSampleApp(VertxTestContext context) {
        assertBootstrap();
        assertRoute();

        var endpoint = String.format("https://%s", route.getSpec().getHost());
        var client = new QuarkusSample(vertx, endpoint);

        LOGGER.info("start streaming prices from: {}", endpoint);

        // The /prices/stream endpoint returns a new price every 5s if the
        // the quarkus app can connect successfully to the kafka instance
        //
        // The test will wait until it receive 6 price consecutively
        // and then it will complete

        var reg = Pattern.compile("^data: (?<data>\\d+(\\.\\d+)?)$");

        List<String> prices = new ArrayList<>();
        WriteStream<Buffer> wsc = WriteStreamConsumer.create(buffer -> {

            var s = buffer.toString();
            for (var line : s.split("\\r?\\n")) {
                var match = reg.matcher(line);
                if (match.matches()) {

                    var price = match.group("data");
                    LOGGER.info("match data: {}", price);

                    // add the received price to the list of prices
                    // and complete the request once we have 6 prices (~30s) by return an error
                    prices.add(price);
                    if (prices.size() > 6) {
                        return Future.failedFuture("complete");
                    }
                }
            }

            return Future.succeededFuture();
        });

        IsReady<Object> complete = last -> client.streamPrices(wsc)
                .recover(t -> {
                    LOGGER.warn("ignore error: ", t);
                    return succeededFuture();
                })
                .map(___ -> {
                    if (prices.size() > 6) {
                        return Pair.with(true, null);
                    }

                    // clean the data list between each failures
                    prices.clear();

                    return Pair.with(false, null);
                });
        waitFor(vertx, "retry data", ofSeconds(1), ofMinutes(3), complete)
                .onComplete(context.succeedingThenComplete());
    }
}
