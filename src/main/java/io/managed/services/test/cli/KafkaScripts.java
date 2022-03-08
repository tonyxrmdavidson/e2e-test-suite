package io.managed.services.test.cli;

import io.managed.services.test.Environment;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.time.Duration.ofMinutes;

public class KafkaScripts {
    private static final Logger LOGGER = LogManager.getLogger(KafkaScripts.class);

    private static final Duration DEFAULT_TIMEOUT = ofMinutes(1);

    private String binariesWorkdir;
    private Path rootWorkDir;

    private final String kafkaVersion = "3.1.0";
    private final String scalaVersion = "2.13";
    private final String kafkaResource = "kafka_" + scalaVersion + "-" + kafkaVersion;


    public KafkaScripts(Path rootWorkDirectory) {
        this.rootWorkDir = rootWorkDirectory;
    }

    // root work dir is returned in order to clean whole temporary dir with all nested content.
    public Path getRootWorkDir() {
        return this.rootWorkDir;
    }

    private ProcessBuilder builder(List<String> command) {

        // retrieve first item which represents script to be executed
        var scriptName = command.get(0);
        var platform = Environment.CLI_PLATFORM;

        var cmd = new ArrayList<String>(command);
        // not tested on widows but may work.
        if (Platform.WINDOWS.toString().equals(platform)) {
            cmd.set(0, ".\\" + scriptName + ".bat");
        } else {
            cmd.set(0, "./" + scriptName + ".sh");
        }

        return new ProcessBuilder(cmd).directory(new File(binariesWorkdir));
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

    public void downloadAndExtractKafkaScripts() throws IOException {

        String kafkaURLString = String.format("https://dlcdn.apache.org/kafka/%s/%s.tgz", kafkaVersion, kafkaResource);
        LOGGER.info("downloading kafka scripts from url: {}", kafkaURLString);

        // where to store downloaded archive
        Path p = Paths.get(rootWorkDir.toString(), "kafka.tgz");
        // alternative download for exec("wget", kafkaURLString, "-P", ".", "--quiet")
        FileUtils.copyURLToFile(
                new URL(kafkaURLString),
                p.toFile(),
                10000,
                10000);

        LOGGER.info("extract binaries");
        try {
            //return new AsyncProcess(builder(command).start());
            var cmd = List.of("tar", "-xvzf", "kafka.tgz");
            var processBuilder = new ProcessBuilder(cmd).directory(new File(rootWorkDir.toString()));
            var startedProcess = processBuilder.start();
            startedProcess.waitFor(10, TimeUnit.SECONDS);

        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        // update command path to the kafka binaries. If windows /bin/windows otherwise /bin
        var platform = Environment.CLI_PLATFORM;

        if (Platform.WINDOWS.toString().equals(platform)) {
            LOGGER.info("move to /bin/windows as bin directory for windows platform");
            this.binariesWorkdir = Paths.get(rootWorkDir.toString(), kafkaResource, "bin", "windows").toString();
        } else {
            LOGGER.info("move to /bin as bin directory for none windows platform");
            this.binariesWorkdir = Paths.get(rootWorkDir.toString(), kafkaResource, "bin").toString();
        }
    }

    public Path createAndSetUpConfigFile(String content) throws ProcessException, IOException {
        String propertiesFile =  "app-services.properties";

        Path pathToNewPropertyFile = Paths.get(rootWorkDir.toString(), kafkaResource, "config", propertiesFile);

        FileWriter fileWriter = new FileWriter(pathToNewPropertyFile.toString());
        PrintWriter printWriter = new PrintWriter(fileWriter);
        printWriter.print(content);
        printWriter.close();

        return pathToNewPropertyFile;
    }

    public String version() throws ProcessException {
        return exec(List.of("kafka-console-consumer", "--version")).stdoutAsString();
    }

    public AsyncProcess kafkaConsoleConsumerAvailable() throws ProcessException {
        return exec(List.of("kafka-console-consumer", "--version"));
    }

    public AsyncProcess kafkaConsoleProducerAvailable() throws ProcessException {
        return exec(List.of("kafka-console-producer", "--version"));
    }

    public AsyncProcess kafkaTopicsAvailable() throws ProcessException {
        return exec(List.of("kafka-topics", "--version"));
    }

    public String createTopic(String topicName, String bootstrapServer, Path propertiesFIle) throws ProcessException {
        return exec(
                "kafka-topics",
                "--create",
                "--topic", topicName,
                "--bootstrap-server", bootstrapServer,
                "--command-config", propertiesFIle.toString()
                ).stdoutAsString();
    }

    public String deleteTopic(String topicName, String bootstrapServer, Path propertiesFIle) throws ProcessException {
        return exec(
                "kafka-topics",
                "--delete",
                "--topic", topicName,
                "--bootstrap-server", bootstrapServer,
                "--command-config", propertiesFIle.toString()
        ).stdoutAsString();
    }

    public AsyncProcess startKafkaConsoleProducer(String topicName, String bootstrapServer, Path propertiesFIle) {

        return execAsync(
                "kafka-console-producer",
                "--topic", topicName,
                "--bootstrap-server", bootstrapServer,
                "--producer.config", propertiesFIle.toString()
        );
    }

    public AsyncProcess startKafkaConsoleConsumer(String topicName, String bootstrapServer, Path propertiesFIle) {

        return execAsync(
                "kafka-console-consumer",
                "--topic", topicName,
                "--bootstrap-server", bootstrapServer,
                "--from-beginning",
                "--consumer.config", propertiesFIle.toString()
        );
    }
}
