package io.managed.services.test.devexp;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.client.DefaultOpenShiftClient;
import io.fabric8.openshift.client.OpenShiftClient;
import io.managed.services.test.Environment;
import io.managed.services.test.IsReady;
import io.managed.services.test.TestBase;
import io.managed.services.test.TestUtils;
import io.managed.services.test.WriteStreamConsumer;
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
import io.managed.services.test.operator.ServiceBindingApplication;
import io.managed.services.test.operator.ServiceBindingSpec;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.streams.WriteStream;
import io.vertx.ext.auth.User;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;
import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
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
import static org.testng.Assert.assertNotNull;

@Test(groups = TestTag.BINDING_OPERATOR)
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

    private static final String TEST_LOGS_PATH = Environment.LOG_DIR.resolve("test-logs").toString();

    private void assertENVs() {
        assertNotNull(Environment.SSO_USERNAME, "the SSO_USERNAME env is null");
        assertNotNull(Environment.SSO_PASSWORD, "the SSO_PASSWORD env is null");
        assertNotNull(Environment.DEV_CLUSTER_SERVER, "the DEV_CLUSTER_SERVER env is null");
        assertNotNull(Environment.DEV_CLUSTER_TOKEN, "the DEV_CLUSTER_TOKEN env is null");
        assertNotNull(Environment.BF2_GITHUB_TOKEN, "the BF2_GITHUB_TOKEN env is null");
    }

    private final Vertx vertx = Vertx.vertx();

    private CLI cli;
    private User user;
    private ServiceAPI api;
    private OpenShiftClient oc;
    private KafkaResponse kafka;
    private Route route;

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

    @BeforeClass(timeOut = 15 * MINUTES)
    public void bootstrap() throws Throwable {
        assertENVs();

        bootstrapK8sClient();

        bwait(bootstrapCLI());

        bwait(bootstrapUserAndAPI());

        bwait(bootstrapKafkaInstance());
    }

    private void collectQuarkusAppLogs(ITestContext context) throws IOException {

        // save Deployment
        var d = oc.apps().deployments().withName(APP_DEPLOYMENT_NAME).get();
        LogCollector.saveObject(TestUtils.getLogPath(TEST_LOGS_PATH, context), d);

        // save Deployment logs
        LogCollector.saveDeploymentLog(
            TestUtils.getLogPath(TEST_LOGS_PATH, context),
            oc, Environment.DEV_CLUSTER_NAMESPACE, APP_DEPLOYMENT_NAME);
    }

    private void collectKafkaConnection(ITestContext context) throws IOException {
        // save KafkaConnection CR before delete it
        var c = OperatorUtils.kafkaConnection(oc).withName(KAFKA_INSTANCE_NAME).get();
        LogCollector.saveObject(TestUtils.getLogPath(TEST_LOGS_PATH, context), c);
    }

    private void collectServiceBinding(ITestContext context) throws IOException {
        // save KafkaConnection CR before delete it
        var b = OperatorUtils.serviceBinding(oc).withName(SERVICE_BINDING_NAME).get();
        LogCollector.saveObject(TestUtils.getLogPath(TEST_LOGS_PATH, context), b);
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

    private void cleanServiceBindingSecrets() {
        for (var secret : oc.secrets().list().getItems()) {
            if (!secret.getMetadata().getName().startsWith(SERVICE_BINDING_NAME)) {
                continue;
            }

            LOGGER.info("clan secret: {}", secret.getMetadata().getName());
            oc.secrets().delete(secret);
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
                .map(a -> {
                    // delete the service account if founded
                    LOGGER.info("delete service account {} with id: {}", a.name, a.id);
                    return api.deleteServiceAccount(a.id);
                })
                .orElseGet(() -> {
                    LOGGER.error("failed to find service account with client-id: {}", secretClientID);
                    return Future.succeededFuture();
                }))

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

    @AfterClass(timeOut = 5 * MINUTES, alwaysRun = true)
    public void teardown(ITestContext context) throws Throwable {
        assumeTeardown();

        try {
            collectQuarkusAppLogs(context);
        } catch (Exception e) {
            LOGGER.error("collectQuarkusAppLogs error: ", e);
        }
        try {
            collectKafkaConnection(context);
        } catch (Exception e) {
            LOGGER.error("collectKafkaConnection error: ", e);
        }
        try {
            collectServiceBinding(context);
        } catch (Exception e) {
            LOGGER.error("collectServiceBinding error: ", e);
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
            cleanServiceBindingSecrets();
        } catch (Exception e) {
            LOGGER.error("cleanServiceBindingSecrets error: ", e);
        }

        try {
            cleanQuarkusSampleApp();
        } catch (Exception e) {
            LOGGER.error("cleanQuarkusSampleApp error: ", e);
        }

        try {
            bwait(cleanServiceAccount());
        } catch (Throwable e) {
            LOGGER.error("cleanServiceAccount error: ", e);
        }

        try {
            bwait(cleanKafkaInstance());
        } catch (Throwable e) {
            LOGGER.error("cleanKafkaInstance error: ", e);
        }

        try {
            bwait(cleanCLI());
        } catch (Throwable e) {
            LOGGER.error("cleanCLI error: ", e);
        }

        bwait(vertx.close());
    }

    @Test(timeOut = DEFAULT_TIMEOUT)
    public void testCLIConnectCluster() throws Throwable {

        cleanAccessTokenSecret();
        cleanKafkaConnection();

        var kubeConfig = CLIUtils.kubeConfig(
            Environment.DEV_CLUSTER_SERVER,
            Environment.DEV_CLUSTER_TOKEN,
            Environment.DEV_CLUSTER_NAMESPACE);

        var config = Serialization.asYaml(kubeConfig);

        var kubeconfgipath = cli.getWorkdir() + "/kubeconfig";
        bwait(vertx.fileSystem().writeFile(kubeconfgipath, Buffer.buffer(config)));

        LOGGER.info("cli use kafka instance: {}", kafka.id);
        bwait(cli.useKafka(kafka.id));

        LOGGER.info("cli cluster connect using kubeconfig: {}", kubeconfgipath);
        bwait(cli.connectCluster(KeycloakOAuth.getRefreshToken(user), kubeconfgipath));
    }

    @Test(dependsOnMethods = "testCLIConnectCluster", timeOut = DEFAULT_TIMEOUT)
    public void testDeployQuarkusSampleApp() {

        LOGGER.info("deploy the rhoas-kafka-quickstart-example app");
        oc.resourceList(loadK8sResources(APP_YAML_PATH)).createOrReplace();

        route = oc.routes().withName(APP_ROUTE_NAME).get();
        LOGGER.info("app deployed to: {}", route.getSpec().getHost());
    }


    @Test(dependsOnMethods = "testDeployQuarkusSampleApp", timeOut = DEFAULT_TIMEOUT)
    public void testCreateServiceBinding() throws Throwable {

        cleanServiceBinding();

        var application = new ServiceBindingApplication();
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

        var sb = new ServiceBinding();
        sb.setMetadata(new ObjectMeta());
        sb.getMetadata().setName(SERVICE_BINDING_NAME);
        sb.setSpec(spec);

        IsReady<Object> serviceBindingIsDeleted = last -> {
            var b = OperatorUtils.serviceBinding(oc).withName(SERVICE_BINDING_NAME).get();

            if (last) {
                LOGGER.warn("last service binding is: {}", Json.encode(b));
            }
            return succeededFuture(Pair.with(b == null, null));
        };

        IsReady<ServiceBinding> serviceBindingIsReady = last -> {
            var b = OperatorUtils.serviceBinding(oc).withName(SERVICE_BINDING_NAME).get();

            if (b == null) {
                return failedFuture(message("the service binding CR doesn't exists anymore: {}", SERVICE_BINDING_NAME));
            }
            if (last) {
                LOGGER.warn("last service binding is: {}", Json.encode(b));
            }

            var isReady = b.getStatus().conditions.stream()
                .filter(c -> c.type.equals("Ready"))
                .findAny()
                .map(c -> c.status.equals("True"))
                .orElse(false);
            return succeededFuture(Pair.with(isReady, b));
        };

        // wait for for the service binding to be gone
        bwait(waitFor(vertx, "service binding to be deleted", ofSeconds(3), ofMinutes(1), serviceBindingIsDeleted));

        LOGGER.info("create service binding: {}", Json.encode(sb));
        OperatorUtils.serviceBinding(oc).create(sb);

        bwait(waitFor(vertx, "service binding to be ready", ofSeconds(3), ofMinutes(1), serviceBindingIsReady));
    }

    @Test(dependsOnMethods = "testCreateServiceBinding", timeOut = 5 * MINUTES)
    public void testQuarkusSampleApp() throws Throwable {

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
                LOGGER.warn("ignore error: {}", t.getMessage());
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
        bwait(waitFor(vertx, "retry data", ofSeconds(1), ofMinutes(3), complete));
    }
}
