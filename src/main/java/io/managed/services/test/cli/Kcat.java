package io.managed.services.test.cli;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.time.Duration.ofMinutes;

public class Kcat {
    private static final Duration DEFAULT_TIMEOUT = ofMinutes(3);

    private final File workdir;
    private final String command;
    private final Map<String, String> environment = new HashMap<>();

    public Kcat(File workdir, String command) {
        this.workdir = workdir;
        this.command = command;
    }

    private ProcessBuilder builder(List<String> command) {
        var cmd = new ArrayList<String>();
        cmd.add(this.command);
        cmd.addAll(command);

        var builder = new ProcessBuilder(cmd)
            .directory(workdir);

        // add or replace the process environment variables
        builder.environment().putAll(environment);

        return builder;
    }

    private AsyncProcess exec(String... command) throws ProcessException {
        return exec(List.of(command));
    }

    private AsyncProcess exec(List<String> command) throws ProcessException {
        return execAsync(command).sync(DEFAULT_TIMEOUT);
    }

    private AsyncProcess execAsync(String... command) {
        return execAsync(List.of(command));
    }

    private AsyncProcess execAsync(List<String> command) {
        try {
            return new AsyncProcess(builder(command).start());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public File getWorkdir() {
        return workdir;
    }

    public void addEnvironment(String key, String value) {
        environment.put(key, value);
    }

    public String getEnvironment(String key) {
        return environment.get(key);
    }

    public String version() throws ProcessException {
        return exec("-V").stdoutAsString();
    }

    public AsyncProcess startProducer(String topic, String bootstrapServer, String username, String password) {
        return execAsync("-t", topic,
            "-b", bootstrapServer,
            "-X", "security.protocol=SASL_SSL",
            "-X", "sasl.mechanisms=PLAIN",
            "-X", String.format("sasl.username=%s", username),
            "-X", String.format("sasl.password=%s", password),
            "-P");
    }

    public AsyncProcess startConsumer(String topic, String bootstrapServer, String username, String password) {
        return execAsync("-t", topic,
            "-b", bootstrapServer,
            "-X", "security.protocol=SASL_SSL",
            "-X", "sasl.mechanisms=PLAIN",
            "-X", String.format("sasl.username=%s", username),
            "-X", String.format("sasl.password=%s", password),
            "-u", // Unbuffered output otherwise messages are flush only after receiving the SIGINT signal
            "-C");
    }
}