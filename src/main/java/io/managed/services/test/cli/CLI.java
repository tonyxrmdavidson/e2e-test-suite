package io.managed.services.test.cli;

import com.openshift.cloud.api.kas.auth.models.AclBindingListPage;
import com.openshift.cloud.api.kas.auth.models.ConsumerGroup;
import com.openshift.cloud.api.kas.auth.models.ConsumerGroupList;
import com.openshift.cloud.api.kas.auth.models.Topic;
import com.openshift.cloud.api.kas.auth.models.TopicsList;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.KafkaRequestList;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import com.openshift.cloud.api.kas.models.ServiceAccountList;
import io.managed.services.test.RetryUtils;
import io.managed.services.test.ThrowingSupplier;
import lombok.SneakyThrows;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static java.time.Duration.ofMinutes;
import static lombok.Lombok.sneakyThrow;

public class CLI {

    private static final Duration DEFAULT_TIMEOUT = ofMinutes(3);

    private final String workdir;
    private final String cmd;

    public CLI(Path binary) {
        this.workdir = binary.getParent().toString();
        this.cmd = String.format("./%s", binary.getFileName().toString());
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

    private AsyncProcess exec(String... command) throws CliGenericException {
        return exec(List.of(command));
    }

    private AsyncProcess exec(List<String> command) throws CliGenericException {
        try {
            return execAsync(command).sync(DEFAULT_TIMEOUT);
        } catch (ProcessException e) {
            throw CliGenericException.exception(e);
        }
    }

    private AsyncProcess execAsync(List<String> command) {
        try {
            return new AsyncProcess(builder(command).start());
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
    }

    /**
     * This method only starts the CLI login, use CLIUtils.login() instead of this method
     * to login using username and password
     */
    public AsyncProcess login(String apiGateway, String authURL, String masAuthURL) {

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

    @SneakyThrows
    public void logout() {
        exec("logout");
    }

    @SneakyThrows
    public String help() {
        return exec("--help").stdoutAsString();
    }

    public KafkaRequest createKafka(String name) throws CliGenericException {
        return retry(() -> exec("kafka", "create", "--name", name))
            .asJson(KafkaRequest.class);
    }

    public void deleteKafka(String id) throws CliGenericException {
        retry(() -> exec("kafka", "delete", "--id", id, "-y"));
    }

    public KafkaRequest describeKafka(String id) throws CliGenericException {
        return retry(() -> exec("kafka", "describe", "--id", id))
            .asJson(KafkaRequest.class);
    }

    public void useKafka(String id) throws CliGenericException {
        retry(() -> exec("kafka", "use", "--id", id));
    }

    public KafkaRequestList listKafka() throws CliGenericException {
        return retry(() -> exec("kafka", "list", "-o", "json"))
            .asJson(KafkaRequestList.class);
    }

    public KafkaRequestList searchKafkaByName(String name) throws CliGenericException {
        var p = retry(() -> exec("kafka", "list", "--search", name, "-o", "json"));
        if (p.stderrAsString().contains("No Kafka instances were found")) {
            return new KafkaRequestList();
        }
        return p.asJson(KafkaRequestList.class);
    }

    public ServiceAccount describeServiceAccount(String id) throws CliGenericException {
        return retry(() -> exec("service-account", "describe", "--id", id))
            .asJson(ServiceAccount.class);
    }

    public ServiceAccountList listServiceAccount() throws CliGenericException {
        return retry(() -> exec("service-account", "list", "-o", "json"))
            .asJson(ServiceAccountList.class);
    }

    public void deleteServiceAccount(String id) throws CliGenericException {
        retry(() -> exec("service-account", "delete", "--id", id, "-y"));
    }

    public void createServiceAccount(String name, Path path) throws CliGenericException {
        retry(() -> exec("service-account", "create", "--short-description", name, "--file-format", "json", "--output-file", path.toString(), "--overwrite"));
    }

    public Topic createTopic(String topicName) throws CliGenericException {
        return retry(() -> exec("kafka", "topic", "create", "--name", topicName, "-o", "json"))
            .asJson(Topic.class);
    }

    public void deleteTopic(String topicName) throws CliGenericException {
        retry(() -> exec("kafka", "topic", "delete", "--name", topicName, "-y"));
    }

    public TopicsList listTopics() throws CliGenericException {
        return retry(() -> exec("kafka", "topic", "list", "-o", "json"))
            .asJson(TopicsList.class);
    }

    public Topic describeTopic(String topicName) throws CliGenericException {
        return retry(() -> exec("kafka", "topic", "describe", "--name", topicName, "-o", "json"))
            .asJson(Topic.class);
    }

    public void updateTopic(String topicName, String retentionTime) throws CliGenericException {
        retry(() -> exec("kafka", "topic", "update", "--name", topicName, "--retention-ms", retentionTime));
    }

    public ConsumerGroupList listConsumerGroups() throws CliGenericException {
        return retry(() -> exec("kafka", "consumer-group", "list", "-o", "json"))
            .asJson(ConsumerGroupList.class);
    }

    public void deleteConsumerGroup(String id) throws CliGenericException {
        retry(() -> exec("kafka", "consumer-group", "delete", "--id", id, "-y"));
    }

    public ConsumerGroup describeConsumerGroup(String name) throws CliGenericException {
        return retry(() -> exec("kafka", "consumer-group", "describe", "--id", name, "-o", "json"))
            .asJson(ConsumerGroup.class);
    }

    public void connectCluster(String token, String kubeconfig, String serviceType) throws CliGenericException {
        retry(() -> exec("cluster", "connect", "--token", token, "--kubeconfig", kubeconfig, "--service-type", serviceType, "-y"));
    }

    public void grantProducerAndConsumerAccess(String userName, String topic, String group) throws CliGenericException {
        retry(() -> exec("kafka", "acl", "grant-access", "-y", "--producer", "--consumer", "--user", userName, "--topic", topic, "--group", group));
    }

    public AclBindingListPage listACLs() throws CliGenericException {
        return retry(() -> exec("kafka", "acl", "list", "-o", "json"))
            .asJson(AclBindingListPage.class);
    }

    private <T, E extends Throwable> T retry(ThrowingSupplier<T, E> call) throws E {
        return RetryUtils.retry(1, call, CLI::retryCondition);
    }

    private static boolean retryCondition(Throwable t) {
        if (t instanceof CliGenericException) {
            return ((CliGenericException) t).getCode() >= 500 && ((CliGenericException) t).getCode() < 600;
        }
        return false;
    }
}
