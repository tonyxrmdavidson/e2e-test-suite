package io.managed.services.test.cli;

import io.managed.services.test.client.serviceapi.KafkaListResponse;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.managed.services.test.client.serviceapi.ServiceAccountList;
import io.managed.services.test.client.serviceapi.TopicListResponse;
import io.managed.services.test.client.serviceapi.TopicResponse;
import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;

import static io.managed.services.test.cli.ProcessUtils.stdout;
import static io.managed.services.test.cli.ProcessUtils.stdoutAsJson;

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

        return new ProcessBuilder(cmd)
                .directory(new File(workdir));
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

    public Future<Process> listKafka() {
        return exec("kafka", "list");
    }

    public Future<KafkaResponse> createKafka(String name) {
        return exec("kafka", "create", name)
                .map(p -> stdoutAsJson(p, KafkaResponse.class));
    }

    public Future<Process> deleteKafka(String id) {
        return exec("kafka", "delete", "--id", id, "-y");
    }

    public Future<KafkaResponse> describeKafka(String id) {
        return exec("kafka", "describe", "--id", id)
                .map(p -> stdoutAsJson(p, KafkaResponse.class));
    }

    public Future<KafkaListResponse> listKafkaAsJson() {
        return exec("kafka", "list", "-o", "json")
                .map(p -> stdoutAsJson(p, KafkaListResponse.class));
    }

    public Future<KafkaListResponse> listKafkaByNameAsJson(String name) {
        return exec("kafka", "list", "--search", name, "-o", "json")
                .map(p -> ProcessUtils.stderr(p).contains("No Kafka instances were found") ?
                        new KafkaListResponse() :
                        stdoutAsJson(p, KafkaListResponse.class));
    }

    public Future<ServiceAccountList> listServiceAccountAsJson() {
        return exec("serviceaccount", "list", "-o", "json")
                .map(p -> stdoutAsJson(p, ServiceAccountList.class));
    }

    public Future<Process> deleteServiceAccount(String id) {
        return exec("serviceaccount", "delete", "--id", id, "-y");
    }

    public Future<Process> createServiceAccount(String name, Path path) {
        return exec("serviceaccount", "create", "--name", name, "--file-format", "json", "--file-location", path.toString(), "--overwrite");
    }

    public Future<TopicResponse> createTopic(String topicName) {
        return exec("kafka", "topic", "create", topicName, "-o", "json").map(p -> stdoutAsJson(p, TopicResponse.class));
    }

    public Future<Process> deleteTopic(String topicName) {
        return exec("kafka", "topic", "delete", topicName, "-y");
    }

    public Future<TopicListResponse> listTopics() {
        return exec("kafka", "topic", "list", "-o", "json")
                .map(p -> stdoutAsJson(p, TopicListResponse.class));
    }

    public Future<TopicResponse> describeTopic(String topicName) {
        return exec("kafka", "topic", "describe", topicName, "-o", "json")
                .map(p -> stdoutAsJson(p, TopicResponse.class));
    }

    public Future<TopicResponse> updateTopic(String topicName, String retentionTime) {
        return exec("kafka", "topic", "update", topicName, "--retention-ms", retentionTime, "-o", "json")
                .map(p -> stdoutAsJson(p, TopicResponse.class));
    }
}
