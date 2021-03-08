package io.managed.services.test.cli;

import io.managed.services.test.client.serviceapi.KafkaListResponse;
import io.managed.services.test.client.serviceapi.KafkaResponse;
import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static io.managed.services.test.cli.ProcessUtils.stdoutAsJson;

public class CLI {
    private static final Logger LOGGER = LogManager.getLogger(CLI.class);

    private final String workdir;
    private final String cmd;

    public CLI(String workdir, String name) {
        this.workdir = workdir;
        this.cmd = String.format("./%s", name);
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

    public Future<Process> listKafka() {
        return exec("kafka", "list");
    }

    public Future<KafkaResponse> createKafka(String name) {
        return exec("kafka", "create", name)
                .map(p -> stdoutAsJson(p, KafkaResponse.class));
    }

    public Future<Process> deleteKafka(String id) {
        return exec("kafka", "delete", "--id", id, "-f");
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
}
