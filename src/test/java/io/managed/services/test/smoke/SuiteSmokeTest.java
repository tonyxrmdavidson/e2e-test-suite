package io.managed.services.test.smoke;

import io.fabric8.kubernetes.api.model.Namespace;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.executor.ExecBuilder;
import io.managed.services.test.executor.ExecResult;
import io.managed.services.test.framework.TestTag;
import io.managed.services.test.k8s.KubeClusterConnectionFactory;
import io.managed.services.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(TestTag.SMOKE_SUITE)
class SuiteSmokeTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(SuiteSmokeTest.class);
    KubeClusterResource cluster;

    @BeforeAll
    void init() {
        cluster = KubeClusterConnectionFactory.connectToKubeCluster(System.getProperty("user.home") + "/.kube/config");
    }

    @Test
    void testListNodes() {
        cluster.kubeClient().listNodes().forEach(node -> LOGGER.info(node.getMetadata().getName()));
        assertTrue(cluster.kubeClient().listNodes().size() > 0);
    }

    @ParameterizedTest(name = "testListNamespace-{0}")
    @ValueSource(strings = {"wrongNamespace", "default"})
    void testGetNamespace(String namespace) {
        LOGGER.info("Getting info from namespace {}", namespace);
        Namespace ns = cluster.kubeClient().getNamespace(namespace);
        assumeTrue(ns != null, "Namespace exists");
        LOGGER.info("Phase of namespace {} is {}", namespace, ns.getStatus().getPhase());
        assertEquals("Namespace", ns.getKind());
    }

    @Test
    void testCmdAndKubeCmd() {
        LOGGER.info("Try ls command without output");
        ExecResult results = new ExecBuilder()
                .withCommand("ls", Environment.SUITE_ROOT)
                .logToOutput(false)
                .exec();
        assertTrue(results.exitStatus());

        LOGGER.info("Try echo command with output");
        results = new ExecBuilder()
                .withCommand("echo", "test output")
                .logToOutput(true)
                .exec();
        assertTrue(results.exitStatus());

        LOGGER.info("Try oc/kubectl command");
        assertTrue(cluster.cmdKubeClient().exec("get", "nodes").exitStatus());
        assertNotNull(cluster.cmdKubeClient().namespace("default").getEvents());
    }
}
