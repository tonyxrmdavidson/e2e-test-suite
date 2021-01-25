package io.managed.services.test.k8s.cmd;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import io.managed.services.test.TestUtils;
import io.managed.services.test.executor.Exec;
import io.managed.services.test.executor.ExecResult;
import io.managed.services.test.k8s.exception.KubeClusterException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.lang.String.join;
import static java.util.Arrays.asList;

public abstract class BaseCmdKubeClient<K extends BaseCmdKubeClient<K>> implements KubeCmdClient<K> {

    private static final Logger LOGGER = LogManager.getLogger(BaseCmdKubeClient.class);

    private static final String CREATE = "create";
    private static final String APPLY = "apply";
    private static final String PROCESS = "process";
    private static final String DELETE = "delete";
    private static final String REPLACE = "replace";

    protected String config;

    String namespace = defaultNamespace();

    protected BaseCmdKubeClient(String config) {
        this.config = config;
    }

    public abstract String cmd();

    @Override
    @SuppressWarnings("unchecked")
    public K deleteByName(String resourceType, String resourceName) {
        Exec.builder()
                .withCommand(namespacedCommand(DELETE, resourceType, resourceName))
                .exec();
        return (K) this;
    }

    protected List<String> namespacedCommand(String... rest) {
        return namespacedCommand(asList(rest));
    }

    private List<String> namespacedCommand(List<String> rest) {
        List<String> result = new ArrayList<>();
        result.add(cmd());
        result.add("--kubeconfig");
        result.add(config);
        result.add("--namespace");
        result.add(namespace);
        result.addAll(rest);
        return result;
    }

    private List<String> command(String... rest) {
        return command(asList(rest));
    }

    private List<String> command(List<String> rest) {
        List<String> result = new ArrayList<>();
        result.add(cmd());
        result.add("--kubeconfig");
        result.add(config);
        result.addAll(rest);
        return result;
    }

    @Override
    public String get(String resource, String resourceName) {
        return Exec.builder()
                .withCommand(namespacedCommand("get", resource, resourceName, "-o", "yaml"))
                .throwErrors(true)
                .exec().out();
    }

    @Override
    public String getEvents() {
        return Exec.builder()
                .withCommand(namespacedCommand("get", "events"))
                .exec().out();
    }

    @Override
    @SuppressWarnings("unchecked")
    public K create(File... files) {
        Map<File, ExecResult> execResults = execRecursive(CREATE, files, Comparator.comparing(File::getName).reversed());
        for (Map.Entry<File, ExecResult> entry : execResults.entrySet()) {
            if (!entry.getValue().exitStatus()) {
                LOGGER.warn("Failed to create {}!", entry.getKey().getAbsolutePath());
                LOGGER.debug(entry.getValue().err());
            }
        }
        return (K) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K apply(File... files) {
        Map<File, ExecResult> execResults = execRecursive(APPLY, files, Comparator.comparing(File::getName).reversed());
        for (Map.Entry<File, ExecResult> entry : execResults.entrySet()) {
            if (!entry.getValue().exitStatus()) {
                LOGGER.warn("Failed to apply {}!", entry.getKey().getAbsolutePath());
                LOGGER.debug(entry.getValue().err());
            }
        }
        return (K) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K apply(String url) {
        ExecResult results = exec(APPLY, "-f", url);
        if (!results.exitStatus()) {
            LOGGER.warn("Failed to apply {}!", url);
            LOGGER.debug(results.err());
        }
        return (K) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K delete(File... files) {
        Map<File, ExecResult> execResults = execRecursive(DELETE, files, Comparator.comparing(File::getName).reversed());
        for (Map.Entry<File, ExecResult> entry : execResults.entrySet()) {
            if (!entry.getValue().exitStatus()) {
                LOGGER.warn("Failed to delete {}!", entry.getKey().getAbsolutePath());
                LOGGER.debug(entry.getValue().err());
            }
        }
        return (K) this;
    }

    private Map<File, ExecResult> execRecursive(String subcommand, File[] files, Comparator<File> cmp) {
        Map<File, ExecResult> execResults = new HashMap<>(25);
        for (File f : files) {
            if (f.isFile()) {
                if (f.getName().endsWith(".yaml")) {
                    ExecResult r = Exec.builder().withCommand(namespacedCommand(subcommand, "-f", f.getAbsolutePath())).logToOutput(false).throwErrors(false).exec();
                    execResults.put(f, r);
                }
            } else if (f.isDirectory()) {
                File[] children = f.listFiles();
                if (children != null) {
                    Arrays.sort(children, cmp);
                    execResults.putAll(execRecursive(subcommand, children, cmp));
                }
            } else if (!f.exists()) {
                throw new RuntimeException(new NoSuchFileException(f.getPath()));
            }
        }
        return execResults;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K replace(File... files) {
        Map<File, ExecResult> execResults = execRecursive(REPLACE, files, Comparator.comparing(File::getName));
        for (Map.Entry<File, ExecResult> entry : execResults.entrySet()) {
            if (!entry.getValue().exitStatus()) {
                LOGGER.warn("Failed to replace {}!", entry.getKey().getAbsolutePath());
                LOGGER.debug(entry.getValue().err());
            }
        }
        return (K) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K applyContent(String yamlContent) {
        Exec.builder()
                .withInput(yamlContent)
                .withCommand(namespacedCommand(APPLY, "-f", "-"))
                .exec();
        return (K) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K process(Map<String, String> parameters, String file, Consumer<String> c) {
        List<String> command = command(PROCESS, "-f", file);
        command.addAll(parameters.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.toList()));
        ExecResult exec = Exec.builder()
                .throwErrors(true)
                .withCommand(command)
                .exec();
        c.accept(exec.out());
        return (K) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K deleteContent(String yamlContent) {
        Exec.builder().withCommand(namespacedCommand(DELETE, "-f", "-")).logToOutput(true).exec();
        return (K) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K createNamespace(String name) {
        Exec.builder()
                .withCommand(namespacedCommand(CREATE, "namespace", name))
                .exec();
        return (K) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K deleteNamespace(String name) {
        Exec.builder().withCommand(namespacedCommand(DELETE, "namespace", name)).logToOutput(true).throwErrors(false);
        return (K) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K scaleByName(String kind, String name, int replicas) {
        Exec.builder()
                .withCommand(namespacedCommand("scale", kind, name, "--replicas", Integer.toString(replicas)))
                .exec();
        return (K) this;
    }

    @Override
    public ExecResult execInPod(String pod, String... command) {
        List<String> cmd = namespacedCommand("exec", pod, "--");
        cmd.addAll(asList(command));
        return Exec.builder()
                .withCommand(cmd)
                .exec();
    }

    @Override
    public ExecResult execInPodContainer(String pod, String container, String... command) {
        return execInPodContainer(true, pod, container, command);
    }

    @Override
    public ExecResult execInPodContainer(boolean logToOutput, String pod, String container, String... command) {
        List<String> cmd = namespacedCommand("exec", pod, "-c", container, "--");
        cmd.addAll(asList(command));
        return Exec.builder()
                .withCommand(cmd)
                .logToOutput(logToOutput)
                .throwErrors(true)
                .exec();
    }

    @Override
    public ExecResult exec(String... command) {
        return exec(true, command);
    }

    @Override
    public ExecResult exec(boolean throwError, String... command) {
        List<String> cmd = new ArrayList<>();
        cmd.add(cmd());
        cmd.add("--kubeconfig");
        cmd.add(config);
        cmd.addAll(asList(command));
        return Exec.builder().withCommand(cmd).throwErrors(throwError).exec();
    }

    @Override
    public ExecResult exec(boolean throwError, boolean logToOutput, String... command) {
        List<String> cmd = new ArrayList<>();
        cmd.add(cmd());
        cmd.add("--kubeconfig");
        cmd.add(config);
        cmd.addAll(asList(command));
        return Exec.builder().withCommand(cmd).logToOutput(logToOutput).throwErrors(throwError).exec();
    }

    @Override
    public ExecResult execInCurrentNamespace(String... commands) {
        return Exec.builder()
                .withCommand(namespacedCommand(commands))
                .exec();
    }

    @Override
    public ExecResult execInCurrentNamespace(boolean logToOutput, String... commands) {
        return Exec.builder()
                .withCommand(namespacedCommand(commands))
                .logToOutput(logToOutput)
                .throwErrors(true)
                .exec();
    }

    @Override
    public ExecResult execInCurrentNamespace(boolean throwError, boolean logToOutput, String... commands) {
        return Exec.builder()
                .withCommand(namespacedCommand(commands))
                .logToOutput(logToOutput)
                .throwErrors(throwError)
                .exec();
    }

    enum ExType {
        BREAK,
        CONTINUE,
        THROW
    }

    @SuppressWarnings("unchecked")
    public K waitFor(String resource, String name, Predicate<JsonNode> condition) {
        long timeoutMs = 570_000L;
        long pollMs = 1_000L;
        ObjectMapper mapper = new ObjectMapper();
        TestUtils.waitFor(resource + " " + name, pollMs, timeoutMs, () -> {
            try {
                String jsonString = Exec.builder()
                        .withCommand(namespacedCommand("get", resource, name, "-o", "json"))
                        .exec().out();
                LOGGER.trace("{}", jsonString);
                JsonNode actualObj = mapper.readTree(jsonString);
                return condition.test(actualObj);
            } catch (KubeClusterException.NotFound e) {
                return false;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return (K) this;
    }

    @Override
    public K waitForResourceCreation(String resourceType, String resourceName) {
        // wait when resource to be created
        return waitFor(resourceType, resourceName,
            actualObj -> true
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public K waitForResourceDeletion(String resourceType, String resourceName) {
        TestUtils.waitFor(resourceType + " " + resourceName + " removal",
                1_000L, 600_000L, () -> {
                try {
                    get(resourceType, resourceName);
                    return false;
                } catch (KubeClusterException.NotFound e) {
                    return true;
                }
            });
        return (K) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K waitForResourceUpdate(String resourceType, String resourceName, Date startTime) {

        TestUtils.waitFor(resourceType + " " + resourceName + " update",
                1_000L, 240_000L, () -> {
                try {
                    return startTime.before(getResourceCreateTimestamp(resourceType, resourceName));
                } catch (KubeClusterException.NotFound e) {
                    return false;
                }
            });
        return (K) this;
    }

    @Override
    public Date getResourceCreateTimestamp(String resourceType, String resourceName) {
        DateFormat df = new SimpleDateFormat("yyyyMMdd'T'kkmmss'Z'");
        Date parsedDate = null;
        try {
            parsedDate = df.parse(JsonPath.parse(getResourceAsJson(resourceType, resourceName)).
                    read("$.metadata.creationTimestamp").toString().replaceAll("\\p{P}", ""));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return parsedDate;
    }

    @Override
    public String toString() {
        return cmd();
    }

    @Override
    public List<String> list(String resourceType) {
        return asList(Exec.builder()
                .withCommand(namespacedCommand("get", resourceType, "-o", "jsonpath={range .items[*]}{.metadata.name} "))
                .exec().out().trim().split(" +")).stream().filter(s -> !s.trim().isEmpty()).collect(Collectors.toList());
    }

    @Override
    public String getResourceAsJson(String resourceType, String resourceName) {
        return Exec.builder()
                .withCommand(namespacedCommand("get", resourceType, resourceName, "-o", "json"))
                .exec().out();
    }

    @Override
    public String getResourceAsYaml(String resourceType, String resourceName) {
        return Exec.builder()
                .withCommand(namespacedCommand("get", resourceType, resourceName, "-o", "yaml"))
                .exec().out();
    }

    @Override
    public void createResourceAndApply(String template, Map<String, String> params) {
        List<String> cmd = namespacedCommand("process", template, "-l", "app=" + template, "-o", "yaml");
        for (Map.Entry<String, String> entry : params.entrySet()) {
            cmd.add("-p");
            cmd.add(entry.getKey() + "=" + entry.getValue());
        }

        String yaml = Exec.builder()
                .withCommand(cmd)
                .exec().out();
        applyContent(yaml);
    }

    @Override
    public String describe(String resourceType, String resourceName) {
        return Exec.builder()
                .withCommand(namespacedCommand("describe", resourceType, resourceName))
                .exec().out();
    }

    @Override
    public String logs(String pod, String container) {
        String[] args;
        if (container != null) {
            args = new String[]{"logs", pod, "-c", container};
        } else {
            args = new String[]{"logs", pod};
        }
        return Exec.builder()
                .withCommand(namespacedCommand(args))
                .exec().out();
    }

    @Override
    public String searchInLog(String resourceType, String resourceName, long sinceSeconds, String... grepPattern) {
        try {
            return Exec.builder()
                    .withCommand("bash", "-c", join(" ", namespacedCommand("logs", resourceType + "/" + resourceName, "--since=" + sinceSeconds + "s",
                            "|", "grep", " -e " + join(" -e ", grepPattern), "-B", "1")))
                    .exec().out();
        } catch (KubeClusterException e) {
            if (e.result != null && e.result.returnCode() == 1) {
                LOGGER.info("{} not found", grepPattern);
            } else {
                LOGGER.error("Caught exception while searching {} in logs", grepPattern);
            }
        }
        return "";
    }

    @Override
    public String searchInLog(String resourceType, String resourceName, String resourceContainer, long sinceSeconds, String... grepPattern) {
        try {
            return Exec.builder()
                    .withCommand("bash", "-c", join(" ", namespacedCommand("logs", resourceType + "/" + resourceName, "-c " + resourceContainer, "--since=" + sinceSeconds + "s",
                            "|", "grep", " -e " + join(" -e ", grepPattern), "-B", "1")))
                    .exec().out();
        } catch (KubeClusterException e) {
            if (e.result != null && e.result.exitStatus()) {
                LOGGER.info("{} not found", grepPattern);
            } else {
                LOGGER.error("Caught exception while searching {} in logs", grepPattern);
            }
        }
        return "";
    }

    public List<String> listResourcesByLabel(String resourceType, String label) {
        return asList(Exec.builder()
                .withCommand(namespacedCommand("get", resourceType, "-l", label, "-o", "jsonpath={range .items[*]}{.metadata.name} "))
                .exec().out().split("\\s+"));
    }
}
