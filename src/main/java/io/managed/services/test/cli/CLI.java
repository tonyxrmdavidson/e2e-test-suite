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
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.managed.services.test.cli.ProcessUtils.stdout;
import static io.managed.services.test.cli.ProcessUtils.stdoutAsJson;
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

    private Process exec(String... command) throws CliGenericException {
        return exec(List.of(command));
    }

    private Process exec(List<String> command) throws CliGenericException {
        try {
            return execAsync(command).future(DEFAULT_TIMEOUT).get();
        } catch (ExecutionException e) {
            var cause = e.getCause();
            if (cause instanceof ProcessException) {
                throw CliGenericException.exception((ProcessException) cause);
            }
            throw sneakyThrow(cause);
        } catch (InterruptedException e) {
            throw sneakyThrow(e);
        }
    }

    @SneakyThrows
    private AsyncProcess execAsync(List<String> command) {
        return new AsyncProcess(builder(command).start());
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
        var p = exec("--help");
        return stdout(p);
    }

    public KafkaRequest createKafka(String name) throws CliGenericException {
        var p = retry(() -> exec("kafka", "create", "--name", name));
        return stdoutAsJson(p, KafkaRequest.class);
    }

    public void deleteKafka(String id) throws CliGenericException {
        retry(() -> exec("kafka", "delete", "--id", id, "-y"));
    }

    public KafkaRequest describeKafka(String id) throws CliGenericException {
        var p = retry(() -> exec("kafka", "describe", "--id", id));
        return stdoutAsJson(p, KafkaRequest.class);
    }

    public void useKafka(String id) throws CliGenericException {
        retry(() -> exec("kafka", "use", "--id", id));
    }

    public KafkaRequestList listKafka() throws CliGenericException {
        var p = retry(() -> exec("kafka", "list", "-o", "json"));
        return stdoutAsJson(p, KafkaRequestList.class);
    }

    public KafkaRequestList searchKafkaByName(String name) throws CliGenericException {
        var p = retry(() -> exec("kafka", "list", "--search", name, "-o", "json"));
        if (ProcessUtils.stderr(p).contains("No Kafka instances were found")) {
            return new KafkaRequestList();
        }
        return stdoutAsJson(p, KafkaRequestList.class);
    }

    public ServiceAccount describeServiceAccount(String id) throws CliGenericException {
        var p = retry(() -> exec("service-account", "describe", "--id", id));
        return stdoutAsJson(p, ServiceAccount.class);
    }

    public ServiceAccountList listServiceAccount() throws CliGenericException {
        var p = retry(() -> exec("service-account", "list", "-o", "json"));
        return stdoutAsJson(p, ServiceAccountList.class);
    }

    public void deleteServiceAccount(String id) throws CliGenericException {
        retry(() -> exec("service-account", "delete", "--id", id, "-y"));
    }

    public void createServiceAccount(String name, Path path) throws CliGenericException {
        retry(() -> exec("service-account", "create", "--short-description", name, "--file-format", "json", "--output-file", path.toString(), "--overwrite"));
    }

    public Topic createTopic(String topicName) throws CliGenericException {
        var p = retry(() -> exec("kafka", "topic", "create", "--name", topicName, "-o", "json"));
        return stdoutAsJson(p, Topic.class);
    }

    public void deleteTopic(String topicName) throws CliGenericException {
        retry(() -> exec("kafka", "topic", "delete", "--name", topicName, "-y"));
    }

    public TopicsList listTopics() throws CliGenericException {
        var p = retry(() -> exec("kafka", "topic", "list", "-o", "json"));
        return stdoutAsJson(p, TopicsList.class);
    }

    public Topic describeTopic(String topicName) throws CliGenericException {
        var p = retry(() -> exec("kafka", "topic", "describe", "--name", topicName, "-o", "json"));
        return stdoutAsJson(p, Topic.class);
    }

    public void updateTopic(String topicName, String retentionTime) throws CliGenericException {
        retry(() -> exec("kafka", "topic", "update", "--name", topicName, "--retention-ms", retentionTime));
    }

    public ConsumerGroupList listConsumerGroups() throws CliGenericException {
        var p = retry(() -> exec("kafka", "consumer-group", "list", "-o", "json"));
        return stdoutAsJson(p, ConsumerGroupList.class);
    }

    public void deleteConsumerGroup(String id) throws CliGenericException {
        retry(() -> exec("kafka", "consumer-group", "delete", "--id", id, "-y"));
    }

    public ConsumerGroup describeConsumerGroup(String name) throws CliGenericException {
        var p = retry(() -> exec("kafka", "consumer-group", "describe", "--id", name, "-o", "json"));
        return stdoutAsJson(p, ConsumerGroup.class);
    }

    public void connectCluster(String token, String kubeconfig, String serviceType) throws CliGenericException {
        retry(() -> exec("cluster", "connect", "--token", token, "--kubeconfig", kubeconfig, "--service-type", serviceType, "-y"));
    }

    public void grantProducerAndConsumerAccess(String userName, String topic, String group) throws CliGenericException {
        retry(() -> exec("kafka", "acl", "grant-access", "-y", "--producer", "--consumer", "--user", userName, "--topic", topic, "--group", group));
    }

    public AclBindingListPage listACLs() throws CliGenericException {
        var p = retry(() -> exec("kafka", "acl", "list", "-o", "json"));
        return stdoutAsJson(p, AclBindingListPage.class);
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
