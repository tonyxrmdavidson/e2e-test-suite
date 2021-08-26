package io.managed.services.test.cli;

import io.managed.services.test.TestUtils;
import io.managed.services.test.client.serviceapi.ConsumerGroupListResponse;
import io.managed.services.test.client.serviceapi.ConsumerGroupResponse;
import io.managed.services.test.client.serviceapi.KafkaListResponse;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAccountList;
import io.managed.services.test.client.serviceapi.TopicListResponse;
import io.managed.services.test.client.serviceapi.TopicResponse;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.managed.services.test.cli.ProcessUtils.stdout;
import static io.managed.services.test.cli.ProcessUtils.stdoutAsJson;
import static java.time.Duration.ofMinutes;

public class CLI {
    private static final Logger LOGGER = LogManager.getLogger(CLI.class);

    private static final Duration DEFAULT_TIMEOUT = ofMinutes(3);

    private final Vertx vertx;
    private final String workdir;
    private final String cmd;

    public CLI(Vertx vertx, String workdir, String name) {
        this.vertx = vertx;
        this.workdir = workdir;
        this.cmd = String.format("./%s", name);
    }

    public String getWorkdir() {
        return this.workdir;
    }

    private ProcessBuilder builder(List<String> command) {
        var cmd = new ArrayList<String>();
        cmd.add(this.cmd);
        cmd.add("-v");
        cmd.addAll(command);

        return new ProcessBuilder(cmd)
            .directory(new File(workdir));
    }

    private Future<Process> exec(String... command) {
        return exec(List.of(command));
    }

    private Future<Process> exec(List<String> command) {
        return execAsync(command).compose(c -> c.future(DEFAULT_TIMEOUT));
    }

    private Future<AsyncProcess> execAsync(String... command) {
        return execAsync(List.of(command));
    }

    private Future<AsyncProcess> execAsync(List<String> command) {
        try {
            return Future.succeededFuture(new AsyncProcess(vertx, builder(command).start()));
        } catch (IOException e) {
            return Future.failedFuture(e);
        }
    }

    /**
     * This method only starts the CLI login, use CLIUtils.login instead of this method
     * to login using username and password
     */
    public Future<AsyncProcess> login(String apiGateway, String authURL, String masAuthURL) {

        List<String> cmd = new ArrayList<>();
        cmd.add("login");

        if (apiGateway != null) {
            cmd.addAll(List.of("--api-gateway", apiGateway));
        }

        if (authURL != null) {
            cmd.addAll(List.of("--auth-url", authURL));
        }

        if (masAuthURL != null) {
            cmd.addAll(List.of("--mas-auth-url", masAuthURL));
        }

        cmd.add("--print-sso-url");

        return execAsync(cmd);
    }

    public Future<AsyncProcess> login() {
        return login(null, null, null);
    }

    public Future<Process> logout() {
        return exec("logout");
    }

    public Future<String> help() {
        return exec("--help")
            .map(p -> stdout(p));
    }

    public Future<Process> listKafka() {
        return retry(() -> exec("kafka", "list"));
    }

    public Future<KafkaResponse> createKafka(String name) {
        return retry(() -> exec("kafka", "create", name)
            .map(p -> stdoutAsJson(p, KafkaResponse.class)));
    }

    public Future<Process> deleteKafka(String id) {
        return retry(() -> exec("kafka", "delete", "--id", id, "-y"));
    }

    public Future<KafkaResponse> describeKafka(String id) {
        return retry(() -> exec("kafka", "describe", "--id", id)
            .map(p -> stdoutAsJson(p, KafkaResponse.class)));
    }

    public Future<Process> useKafka(String id) {
        return retry(() -> exec("kafka", "use", "--id", id));
    }

    public Future<KafkaListResponse> listKafkaAsJson() {
        return retry(() -> exec("kafka", "list", "-o", "json")
            .map(p -> stdoutAsJson(p, KafkaListResponse.class)));
    }

    public Future<KafkaListResponse> listKafkaByNameAsJson(String name) {
        return retry(() -> exec("kafka", "list", "--search", name, "-o", "json")
            .map(p -> ProcessUtils.stderr(p).contains("No Kafka instances were found") ?
                new KafkaListResponse() :
                stdoutAsJson(p, KafkaListResponse.class)));
    }

    public Future<ServiceAccountList> listServiceAccountAsJson() {
        return retry(() -> exec("service-account", "list", "-o", "json")
            .map(p -> stdoutAsJson(p, ServiceAccountList.class)));
    }

    public Future<Process> deleteServiceAccount(String id) {
        return retry(() -> exec("service-account", "delete", "--id", id, "-y"));
    }

    public Future<Process> createServiceAccount(String name, Path path) {
        return retry(() -> exec("service-account", "create", "--name", name, "--file-format", "json", "--file-location", path.toString(), "--overwrite"));
    }

    public Future<TopicResponse> createTopic(String topicName) {
        return retry(() -> exec("kafka", "topic", "create", "--name", topicName, "-o", "json").map(p -> stdoutAsJson(p, TopicResponse.class)));
    }

    public Future<Process> deleteTopic(String topicName) {
        return retry(() -> exec("kafka", "topic", "delete", "--name", topicName, "-y"));
    }

    public Future<TopicListResponse> listTopics() {
        return retry(() -> exec("kafka", "topic", "list", "-o", "json")
            .map(p -> stdoutAsJson(p, TopicListResponse.class)));
    }

    public Future<TopicResponse> describeTopic(String topicName) {
        return retry(() -> exec("kafka", "topic", "describe", "--name", topicName, "-o", "json")
            .map(p -> stdoutAsJson(p, TopicResponse.class)));
    }

    public Future<TopicResponse> updateTopic(String topicName, String retentionTime) {
        return retry(() -> exec("kafka", "topic", "update", "--name", topicName, "--retention-ms", retentionTime, "-o", "json")
            .map(p -> stdoutAsJson(p, TopicResponse.class)));
    }

    public Future<ConsumerGroupListResponse> listConsumerGroups() {
        return retry(() -> exec("kafka", "consumer-group", "list", "-o", "json")
                .map(p -> stdoutAsJson(p, ConsumerGroupListResponse.class)));
    }

    public Future<Process> deleteConsumerGroup(String id) {
        return retry(() -> exec("kafka", "consumer-group", "delete", "--id", id, "-y"));
    }

    public Future<ConsumerGroupResponse> describeConsumerGroup(String name) {
        return retry(() -> exec("kafka", "consumer-group", "describe", "--id", name, "-o", "json")
                .map(p -> stdoutAsJson(p, ConsumerGroupResponse.class)));
    }

    public Future<Process> connectCluster(String token, String kubeconfig) {
        return retry(() -> exec("cluster", "connect", "--token", token, "--kubeconfig", kubeconfig, "-y"));
    }

    public <T> Future<T> retry(Supplier<Future<T>> call) {

        Function<Throwable, Boolean> condition = t -> {
            if (t instanceof ProcessException) {
                if (t.getMessage().contains("504")
                    || t.getMessage().contains("500")
                    || t.getMessage().contains("503")
                    || t.getMessage().contains("internal")) {
                    return true;
                }
            }

            return false;
        };

        return TestUtils.retry(vertx, call, condition);
    }
}
