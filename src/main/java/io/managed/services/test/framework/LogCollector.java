package io.managed.services.test.framework;

import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class LogCollector {
    private static final Logger LOGGER = LogManager.getLogger(LogCollector.class);

    public static void saveDeploymentLog(Path logpath, KubernetesClient client, String namespace, String deploymentName) throws IOException {
        LOGGER.info("Saving log for deployment {} into {}", deploymentName, logpath.toString());
        Files.createDirectories(logpath);
        Files.writeString(logpath.resolve(String.format("%s-%s.log", deploymentName, namespace)),
                client.apps().deployments().inNamespace(namespace).withName(deploymentName).getLog());
    }
}
