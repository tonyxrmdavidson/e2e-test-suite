package io.managed.services.test.kafka;

import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import com.openshift.cloud.api.kas.models.ServiceAccountRequest;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.oauth.KeycloakLoginSession;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
import lombok.SneakyThrows;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.DelegationTokenDisabledException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

/**
 * <p>
 * <b>Requires:</b>
 * <ul>
 *     <li> PRIMARY_USERNAME
 *     <li> PRIMARY_PASSWORD
 *     <li> SECONDARY_USERNAME
 *     <li> SECONDARY_PASSWORD
 *     <li> ALIEN_USERNAME
 *     <li> ALIEN_PASSWORD
 * </ul>
 */
public class KafkaAccessControlTest extends TestBase {
    // TODO At least all described tests in the document are automated
    //TODO KafkaMgmtAPIPermissionTest (migrate completely)
    //TODO KafkaAdminPermissionTest (migrate completely)
    //TODO KafkaInstanceAPITest (migrate all permission tests)

    private static final Logger LOGGER = LogManager.getLogger(KafkaAccessControlTest.class);

    //private static final String KAFKA_INSTANCE_NAME = "mk-e2e-ac-" + Environment.LAUNCH_KEY;
    // TODO change to previous name
    private static final String KAFKA_INSTANCE_NAME = "mk-e2e-up-" + Environment.LAUNCH_KEY;
    private static final String PRIMARY_SERVICE_ACCOUNT_NAME = "mk-e2e-ac-primary-sa-" + Environment.LAUNCH_KEY;
    private static final String SERVICE_ACCOUNT_NAME = PRIMARY_SERVICE_ACCOUNT_NAME;
    private static final String SECONDARY_SERVICE_ACCOUNT_NAME = "mk-e2e-ac-secondary-sa-" + Environment.LAUNCH_KEY;
    private static final String ALIEN_SERVICE_ACCOUNT_NAME = "mk-e2e-ac-alien-sa-" + Environment.LAUNCH_KEY;

    private static final String TOPIC_NAME_FOR_GROUPS = "temporary-topic-name";

    private ApplicationServicesApi mainAPI;
    private ApplicationServicesApi secondaryAPI;
    private ApplicationServicesApi alienAPI;
    private ApplicationServicesApi adminAPI;

    private ServiceAccount mainServiceAccount;
    private ServiceAccount secondaryServiceAccount;
    private ServiceAccount alienServiceAccount;

    private KafkaRequest kafka;

    private KafkaMgmtApi kafkaMgmtApi;
    //private SecurityMgmtApi securityMgmtApi;
    private KafkaInstanceApi kafkaInstanceApi;

    private KafkaAdmin admin;

    @BeforeClass
    @SneakyThrows
    public void bootstrap() {
        assertNotNull(Environment.ADMIN_USERNAME, "the ADMIN_USERNAME env is null");
        assertNotNull(Environment.ADMIN_PASSWORD, "the ADMIN_PASSWORD env is null");
        assertNotNull(Environment.PRIMARY_USERNAME, "the PRIMARY_USERNAME env is null");
        assertNotNull(Environment.PRIMARY_PASSWORD, "the PRIMARY_PASSWORD env is null");
        assertNotNull(Environment.SECONDARY_USERNAME, "the SECONDARY_USERNAME env is null");
        assertNotNull(Environment.SECONDARY_PASSWORD, "the SECONDARY_PASSWORD env is null");
        assertNotNull(Environment.ALIEN_USERNAME, "the ALIEN_USERNAME env is null");
        assertNotNull(Environment.ALIEN_PASSWORD, "the ALIEN_PASSWORD env is null");

        mainAPI = ApplicationServicesApi.applicationServicesApi(
                Environment.PRIMARY_USERNAME,
                Environment.PRIMARY_PASSWORD);

        secondaryAPI = ApplicationServicesApi.applicationServicesApi(
                Environment.SECONDARY_USERNAME,
                Environment.SECONDARY_PASSWORD);

        alienAPI = ApplicationServicesApi.applicationServicesApi(
                Environment.ALIEN_USERNAME,
                Environment.ALIEN_PASSWORD);

        adminAPI = ApplicationServicesApi.applicationServicesApi(
                Environment.ADMIN_USERNAME,
                Environment.ADMIN_PASSWORD);

        LOGGER.info("create kafka instance '{}'", KAFKA_INSTANCE_NAME);
        kafka = KafkaMgmtApiUtils.applyKafkaInstance(mainAPI.kafkaMgmt(), KAFKA_INSTANCE_NAME);


        kafkaMgmtApi = mainAPI.kafkaMgmt();
        //securityMgmtApi = mainAPI.securityMgmt();

        ServiceAccount secondaryServiceAccount =
                SecurityMgmtAPIUtils.applyServiceAccount(secondaryAPI.securityMgmt(), SECONDARY_SERVICE_ACCOUNT_NAME);
        ServiceAccount serviceAccount =
                SecurityMgmtAPIUtils.applyServiceAccount(mainAPI.securityMgmt(), SERVICE_ACCOUNT_NAME);

        // create the kafka admin
        admin = new KafkaAdmin(
                kafka.getBootstrapServerHost(),
                serviceAccount.getClientId(),
                serviceAccount.getClientSecret());
        LOGGER.info("kafka admin api initialized for instance: {}", kafka.getBootstrapServerHost());
    }


    public void teardown() {

        if (admin != null) {
            // close KafkaAdmin
            admin.close();
        }

        assumeTeardown();

        try {
            KafkaMgmtApiUtils.cleanKafkaInstance(adminAPI.kafkaMgmt(), KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            LOGGER.error("clan kafka error: ", t);
        }
        try {
            SecurityMgmtAPIUtils.cleanServiceAccount(mainAPI.securityMgmt(), PRIMARY_SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean main (primary) service account error: ", t);
        }

        try {
            SecurityMgmtAPIUtils.cleanServiceAccount(secondaryAPI.securityMgmt(), SECONDARY_SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean secondary service account error: ", t);
        }

        try {
            SecurityMgmtAPIUtils.cleanServiceAccount(alienAPI.securityMgmt(), ALIEN_SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean alien service account error: ", t);
        }

    }

    @Test
    @SneakyThrows
    public void testSecondaryUserCanReadTheKafkaInstance() {

        // Get kafka instance list by another user with same org
        LOGGER.info("fetch list of kafka instance from the secondary user in the same org");
        var kafkas = secondaryAPI.kafkaMgmt().getKafkas(null, null, null, null);

        LOGGER.debug(kafkas);

        var o = kafkas.getItems().stream()
                .filter(k -> KAFKA_INSTANCE_NAME.equals(k.getName()))
                .findAny();
        assertTrue(o.isPresent());
    }

    @Test
    @SneakyThrows
    public void testAlienUserCanNotReadTheKafkaInstance() {

        // Get list of kafka Instance in org 1 and test it should be there
        LOGGER.info("fetch list of kafka instance from the alin user in a different org");
        var kafkas = alienAPI.kafkaMgmt().getKafkas(null, null, null, null);

        LOGGER.debug(kafkas);

        var o = kafkas.getItems().stream()
                .filter(k -> KAFKA_INSTANCE_NAME.equals(k.getName()))
                .findAny();
        assertTrue(o.isEmpty());
    }

    // always denied operations
    @Test
    public void testForbiddenToCreateDelegationToken() {

        LOGGER.info("kafka-delegation-tokens.sh create <forbidden>, script representation test");
        assertThrows(DelegationTokenDisabledException.class, () -> admin.createDelegationToken());
    }

    @Test
    public void testForbiddenToDescribeDelegationToken() {

        LOGGER.info("kafka-delegation-tokens.sh describe <forbidden>, script representation test");
        assertThrows(DelegationTokenDisabledException.class, () -> admin.describeDelegationToken());
    }

    @Test
    public void testForbiddenToUncleanLeaderElection() {

        LOGGER.info("kafka-leader-election.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> admin.electLeader(ElectionType.UNCLEAN, TOPIC_NAME_FOR_GROUPS));
    }

    @Test
    public void testForbiddenToDescribeLogDirs() {

        LOGGER.info("kafka-log-dirs.sh --describe <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> admin.logDirs());
    }

    @Test
    public void testForbiddenToAlterPreferredReplicaElection() {

        LOGGER.info("kafka-preferred-replica-election.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> admin.electLeader(ElectionType.PREFERRED, TOPIC_NAME_FOR_GROUPS));
    }

    @Test
    public void testForbiddenToReassignPartitions() {

        LOGGER.info("kafka-reassign-partitions.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> admin.reassignPartitions(TOPIC_NAME_FOR_GROUPS));
    }

}
