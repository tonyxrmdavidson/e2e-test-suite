package io.managed.services.test.cli;

import io.managed.services.test.Environment;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.managed.services.test.TestUtils.sleep;
import static io.managed.services.test.cli.ProcessUtils.stdout;
import static io.managed.services.test.cli.ProcessUtils.stdoutAsJson;
import static java.time.Duration.ofSeconds;

public class CLI {
    private static final Logger LOGGER = LogManager.getLogger(CLI.class);

    private final String workdir;
    private final String cmd;

    public CLI(String workdir, String name) {
        this.workdir = workdir;
        this.cmd = String.format("./%s", name);
    }

    public String getWorkdir() {
        return this.workdir;
    }

    private ProcessBuilder builder(String... command) {
        var cmd = new ArrayList<String>();
        cmd.add(this.cmd);
        cmd.add("-d");
        cmd.addAll(Arrays.asList(command));

        ProcessBuilder pb = new ProcessBuilder(cmd)
                .directory(new File(workdir));
        //TODO remove env setting after fix for https://github.com/bf2fc6cc711aee1a0c2a/cli/issues/497 will be released
        Map<String, String> env = pb.environment();
        env.put("RHOASCLI_CONFIG", System.getenv("HOME") + "/.rhoascli.json");
        return pb;
    }

    private Future<Process> exec(String... command) {
        return execAsync(command).compose(AsyncProcess::future);
    }

    private Future<AsyncProcess> execAsync(String... command) {
        try {
            return Future.succeededFuture(new AsyncProcess(builder(command).start()));
        } catch (IOException e) {
            return Future.failedFuture(e);
        }
    }

    /**
     * This method only starts the CLI login, use CLIUtils.login instead of this method
     * to login using username and password
     */
    public Future<AsyncProcess> login() {
        return execAsync("login", "--print-sso-url");
    }

    public Future<Process> logout() {
        return exec("logout");
    }

    public Future<String> help() {
        return exec("--help")
                .map(p -> stdout(p));
    }

    public Future<Process> listKafka(Vertx vertx) {
        return retry(vertx, () -> exec("kafka", "list"));
    }

    public Future<KafkaResponse> createKafka(Vertx vertx, String name) {
        return retry(vertx, () -> exec("kafka", "create", name)
                .map(p -> stdoutAsJson(p, KafkaResponse.class)));
    }

    public Future<Process> deleteKafka(Vertx vertx, String id) {
        return retry(vertx, () -> exec("kafka", "delete", "--id", id, "-y"));
    }

    public Future<KafkaResponse> describeKafka(Vertx vertx, String id) {
        return retry(vertx, () -> exec("kafka", "describe", "--id", id)
                .map(p -> stdoutAsJson(p, KafkaResponse.class)));
    }

    public Future<KafkaListResponse> listKafkaAsJson(Vertx vertx) {
        return retry(vertx, () -> exec("kafka", "list", "-o", "json")
                .map(p -> stdoutAsJson(p, KafkaListResponse.class)));
    }

    public Future<KafkaListResponse> listKafkaByNameAsJson(Vertx vertx, String name) {
        return retry(vertx, () -> exec("kafka", "list", "--search", name, "-o", "json")
                .map(p -> ProcessUtils.stderr(p).contains("No Kafka instances were found") ?
                        new KafkaListResponse() :
                        stdoutAsJson(p, KafkaListResponse.class)));
    }

    public Future<ServiceAccountList> listServiceAccountAsJson(Vertx vertx) {
        return retry(vertx, () -> exec("serviceaccount", "list", "-o", "json")
                .map(p -> stdoutAsJson(p, ServiceAccountList.class)));
    }

    public Future<Process> deleteServiceAccount(Vertx vertx, String id) {
        return retry(vertx, () -> exec("serviceaccount", "delete", "--id", id, "-y"));
    }

    public Future<Process> createServiceAccount(Vertx vertx, String name, Path path) {
        return retry(vertx, () -> exec("serviceaccount", "create", "--name", name, "--file-format", "json", "--file-location", path.toString(), "--overwrite"));
    }

    public Future<TopicResponse> createTopic(Vertx vertx, String topicName) {
        return retry(vertx, () -> exec("kafka", "topic", "create", topicName, "-o", "json").map(p -> stdoutAsJson(p, TopicResponse.class)));
    }

    public Future<Process> deleteTopic(Vertx vertx, String topicName) {
        return retry(vertx, () -> exec("kafka", "topic", "delete", topicName, "-y"));
    }

    public Future<TopicListResponse> listTopics(Vertx vertx) {
        return retry(vertx, () -> exec("kafka", "topic", "list", "-o", "json")
                .map(p -> stdoutAsJson(p, TopicListResponse.class)));
    }

    public Future<TopicResponse> describeTopic(Vertx vertx, String topicName) {
        return retry(vertx, () -> exec("kafka", "topic", "describe", topicName, "-o", "json")
                .map(p -> stdoutAsJson(p, TopicResponse.class)));
    }

    public Future<TopicResponse> updateTopic(Vertx vertx, String topicName, String retentionTime) {
        return retry(vertx, () -> exec("kafka", "topic", "update", topicName, "--retention-ms", retentionTime, "-o", "json")
                .map(p -> stdoutAsJson(p, TopicResponse.class)));
    }

    public <T> Future<T> retry(Vertx vertx, Supplier<Future<T>> call) {
        return retry(vertx, call, Environment.API_CALL_THRESHOLD);
    }

    public <T> Future<T> retry(Vertx vertx, Supplier<Future<T>> call, int attempts) {

        Function<Throwable, Future<T>> retry = t -> {
            LOGGER.error("skip error: ", t);

            // retry the CLI call
            return sleep(vertx, ofSeconds(1))
                    .compose(r -> retry(vertx, call, attempts - 1));
        };

        return call.get().recover(t -> {
            if (attempts <= 0) {
                // no more attempts remaining
                return Future.failedFuture(t);
            }

            if (t instanceof ProcessException) {
                if (t.getMessage().contains("504") || t.getMessage().contains("500")) {
                    return retry.apply(t);
                }
            }

            return Future.failedFuture(t);
        });
    }
}
