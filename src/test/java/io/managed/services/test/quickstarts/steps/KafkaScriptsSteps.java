package io.managed.services.test.quickstarts.steps;

import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.managed.services.test.cli.AsyncProcess;
import io.managed.services.test.cli.KafkaScripts;
import io.managed.services.test.cli.ProcessException;
import io.managed.services.test.quickstarts.contexts.KafkaInstanceContext;
import io.managed.services.test.quickstarts.contexts.ServiceAccountContext;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.io.FileUtils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static org.testng.Assert.assertTrue;


@Log4j2
public class KafkaScriptsSteps {

    private KafkaScripts kafkaScripts;
    private static final String TMPDIR = "kafka-scripts";

    private Path kafkaPropertiesPath;
    private String topicName;

    private final KafkaInstanceContext kafkaInstanceContext;
    private final ServiceAccountContext serviceAccountContext;

    private AsyncProcess producerProcess;
    private AsyncProcess consumerProcess;
    private boolean exampleMessagesProduced;

    public KafkaScriptsSteps(KafkaInstanceContext kafkaInstanceContext, ServiceAccountContext serviceAccountContext) {
        this.kafkaInstanceContext = kafkaInstanceContext;
        this.serviceAccountContext = serviceAccountContext;
    }

    @When("you create an `app-services.properties` file with SASL connection mechanism")
    public void youCreateAnAppServicesPropertiesFileWithSASLConnectionMechanism() throws IOException, ProcessException {

        var sa = serviceAccountContext.requireServiceAccount();

        var propertiesString =
                "sasl.mechanism=PLAIN\n" +
                "security.protocol=SASL_SSL\n" +
                "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required " +
                    String.format("username=\"%s\" ", sa.getClientId()) +
                    String.format("password=\"%s\" ;", sa.getClientSecret());

        kafkaPropertiesPath = kafkaScripts.createAndSetUpConfigFile(propertiesString);
    }

    @When("you produce messages to the topic {word} using `kafka-console-producer.sh`")
    public void you_produce_messages_to_the_topic_you_created_using_kafka_console_producer_sh(String topicName) throws ProcessException, IOException {

        log.info("start producer using kafka-console-producer.sh script");
        var bootstrapServerHost = kafkaInstanceContext.requireKafkaInstance().getBootstrapServerHost();
        producerProcess = kafkaScripts.startKafkaConsoleProducer(topicName, bootstrapServerHost, kafkaPropertiesPath);

        var stdin = producerProcess.stdin();

        log.info("write example messages");
        stdin.write("First message\n");
        stdin.write("Second message\n");
        stdin.write("Third message\n");

        stdin.close();

        exampleMessagesProduced = true;
        log.info("kafka console producer started");

    }

    public AsyncProcess requireProducer() {
        return Objects.requireNonNull(producerProcess);
    }

    public AsyncProcess requireConsumer() {
        return Objects.requireNonNull(consumerProcess);
    }

    @When("you consume messages from the topic {word} you created using `kafka-console-consumer.sh`")
    public void you_consume_messages_from_the_topic_you_created_using_kafka_console_consumer_sh(String topicName) {

        log.info("start the kafka consumer script");
        var bootstrapServerHost = kafkaInstanceContext.requireKafkaInstance().getBootstrapServerHost();
        consumerProcess = kafkaScripts.startKafkaConsoleConsumer(topicName, bootstrapServerHost, kafkaPropertiesPath);

        log.info("kafka-console-consumer started");
    }

    @Then("your `kafka-console-consumer` is running without any errors")
    public void yourKafkaConsoleConsumerIsRunningWithoutAnyErrors() throws Throwable {

        var consumer = requireConsumer();
        if (consumer.isDead()) {
            log.error("the producer is dead");

            // If the process has failed with exit code != 0 it will throw a CliProcessException here
            consumer.sync(Duration.ofSeconds(1));

            // otherwise
            throw new Exception("the process is dead but didn't fail", consumer.processException(null));
        }
        log.info("the consumer is still alive");
    }

    @Then("the `kafka-console-consumer` display the example messages from the producer")
    public void theKafkaConsoleConsumerDisplayTheExampleMessagesFromTheProducer() throws Throwable {
        var consumer = requireConsumer();

        var expectedMessages = new ArrayList<String>();
        expectedMessages.add("First message");
        expectedMessages.add("Second message");
        expectedMessages.add("Third message");

        var stdout = consumer.stdout();

        var consumerFuture = consumer.future(Duration.ofMinutes(1));

        var parserFuture = CompletableFuture.runAsync(() -> {
            log.info("start message parser");

            do {
                try {
                    var l = stdout.readLine();
                    if (l == null) {
                        log.info("end of file");
                        break;
                    }

                    log.info("consumer message: {}", l);

                    if (expectedMessages.remove(l)) {
                        log.info("matched message '{}'", l);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } while (!expectedMessages.isEmpty());

            log.info("message parser finished");
        });

        CompletableFuture.anyOf(parserFuture, consumerFuture).get();

        log.info("parse messages completed");
        assertEquals(expectedMessages, new ArrayList<>());
    }

    @Then("the `kafka-console-producer` is still running without any errors")
    public void the_kafka_console_producer_is_still_running_without_any_errors() throws Throwable {

        var producer = requireProducer();
        if (producer.isDead()) {
            log.error("the producer is dead");

            // If the process has failed with exit code != 0 it will throw a CliProcessException here
            producer.sync(Duration.ofSeconds(1));

            // otherwise
            throw new Exception("the process is dead but didn't fail", producer.processException(null));
        }
        log.info("the producer is still alive");
    }

    @Given("you have downloaded and verified the latest supported binary version of the Apache Kafka distribution")
    public void you_have_downloaded_and_verified_the_latest_supported_binary_version_of_the_apache_kafka_distribution() throws Throwable {

        log.info("download CLI in temporary directory");

        var temporaryWorkdir = Files.createTempDirectory(TMPDIR);
        log.info("created tmp dir: {}", temporaryWorkdir.toString());

        // new KafkaScripts class that operates in temporary dir
        kafkaScripts = new KafkaScripts(temporaryWorkdir);
        // after this method directory is changed to /bin so all scripts can be executed as from within current dir.
        kafkaScripts.downloadAndExtractKafkaScripts();

        var version = kafkaScripts.version();
        log.info(version);
    }

    @When("you enter a command to create Kafka topic {word} using `kafka-topics.sh`")
    public void you_enter_a_command_to_create_a_kafka_topic_using_kafka_topics_sh(String topicName) throws ProcessException {

        this.topicName = topicName;
        var bootstrapHost = kafkaInstanceContext.requireKafkaInstance().getBootstrapServerHost();

        kafkaScripts.createTopic(topicName, bootstrapHost, kafkaPropertiesPath);
    }

    @Given("you used a `kafka-console-producer` to produce example messages to a topic")
    public void you_used_a_kafka_console_producer_to_produce_example_messages_to_a_topic() {
        assertTrue(exampleMessagesProduced);
    }

    @Given("the Kafka topic creation script `kafka-topics.sh` is available")
    public void theKafkatopicCreationScriptKafkaTopicsShIsAvailable() throws ProcessException {
        kafkaScripts.kafkaTopicsAvailable();
    }

    @Given("the Kafka producer creation script `kafka-console-producer.sh` is available")
    public void theKafkaProducerCreationScriptKafkaConsoleProducerShIsAvailable() throws ProcessException {
        kafkaScripts.kafkaConsoleProducerAvailable();
    }

    @Given("the Kafka consumer creation script `kafka-console-consumer.sh` is available")
    public void theKafkaConsumerCreationScriptKafkaCOnsoleConsumerShIsAvailable() throws ProcessException {
        kafkaScripts.kafkaConsoleConsumerAvailable();
    }

    @Given("an `app-services.properties` file is configured")
    public void an_app_services_properties_file_is_configured() {
        assertNotNull(kafkaPropertiesPath);
    }

    @Then("the topic {word} is created in the Kafka instance")
    public void theTopicIsCreatedInTheKafkaInstance(String topicName) {
        assertTrue(topicName.equals(this.topicName));
    }


    @After(order = 10200)
    public void cleanKafkaWorkdir() {

        if (kafkaScripts == null) return;

        assumeTeardown();

        // clean cli workdir
        log.info("delete kafka scripts workdir: {}", kafkaScripts.getRootWorkDir());
        try {
            FileUtils.deleteDirectory(kafkaScripts.getRootWorkDir().toFile());
        } catch (Throwable t) {
            log.error("clean workdir error: ", t);
        }

        kafkaScripts = null;
    }

    @After(order = 10201)
    public void cleanTopic() {

        if (this.kafkaInstanceContext == null || topicName == null) {
            return;
        }

        assumeTeardown();

        // clean cli workdir
        log.info("delete topic: {}", topicName);
        try {
            kafkaScripts.deleteTopic(topicName, this.kafkaInstanceContext.getKafkaInstance().getBootstrapServerHost(), this.kafkaPropertiesPath);
        } catch (Throwable t) {
            log.error("clean topic error: ", t);
        }
    }

    @After(order = 10210)
    public void closeConsumer() {
        if (consumerProcess == null) return;

        try {
            consumerProcess.destroy();
            log.info("consumer output:\n{}", consumerProcess.outputAsString());
        } catch (Throwable t) {
            log.error("close consumer error:", t);
        }

        consumerProcess = null;
    }

    @After(order = 10210)
    public void closeProducer() {
        if (producerProcess == null) return;

        try {
            producerProcess.destroy();
            log.info("producer output:\n{}", producerProcess.outputAsString());
        } catch (Throwable t) {
            log.error("close producer error:", t);
        }

        producerProcess = null;
    }


}
