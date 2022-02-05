package io.managed.services.test.client.kafkainstance;

import com.openshift.cloud.api.kas.auth.models.AclBinding;
import com.openshift.cloud.api.kas.auth.models.AclOperation;
import com.openshift.cloud.api.kas.auth.models.AclOperationFilter;
import com.openshift.cloud.api.kas.auth.models.AclPatternType;
import com.openshift.cloud.api.kas.auth.models.AclPatternTypeFilter;
import com.openshift.cloud.api.kas.auth.models.AclPermissionType;
import com.openshift.cloud.api.kas.auth.models.AclPermissionTypeFilter;
import com.openshift.cloud.api.kas.auth.models.AclResourceType;
import com.openshift.cloud.api.kas.auth.models.AclResourceTypeFilter;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import io.managed.services.test.client.exception.ApiGenericException;
import lombok.extern.log4j.Log4j2;

import java.util.LinkedList;
import java.util.List;

@Log4j2
public class KafkaInstanceApiAccessUtils {

    /**
     * remove all ACLs different from those provided in defaultACLsList
     *
     * @param api       KafkaInstanceApi
     * @param defaultACLsList The list of ACLs which are not to be deleted
     */
    public static void removeAllButDefaultACLs(KafkaInstanceApi api, List<AclBinding> defaultACLsList) throws ApiGenericException {
        // get difference of ACLs that are default from ACLs that are currently present
        var aclPage = api.getAcls(null, null, null, null, null, null, null, null, null, null);
        var acls = aclPage.getItems();
        List<AclBinding> differences = acls;
        differences.removeAll(defaultACLsList);

        // remove extra ACLs one by one
        for (AclBinding aclItem  : differences) {
            api.deleteAcls(
                    AclResourceTypeFilter.valueOf(aclItem.getResourceType().getValue()),
                    null,
                    AclPatternTypeFilter.valueOf(aclItem.getPatternType().getValue()),
                    aclItem.getPrincipal(),
                    AclOperationFilter.valueOf(aclItem.getOperation().getValue()),
                    AclPermissionTypeFilter.valueOf(aclItem.getPermission().getValue())
            );
        }
    }

    /**
     * return list of ACLs currently created on instance
     *
     * @param api       KafkaInstanceApi
     */
    public static List<AclBinding> getDefaultACLs(KafkaInstanceApi api) throws ApiGenericException {
        var aclPage = api.getAcls(null, null, null, null, null, null, null, null, null, null);
        var acls = aclPage.getItems();
        List<AclBinding> defaultPermissionsList = new LinkedList<>();
        for (AclBinding aclItem  : acls) {
            defaultPermissionsList.add(aclItem);
        }
        return defaultPermissionsList;
    }

    /**
     * Convert service account clientID into ACL principal.
     *
     * @param clientID Service account clientID
     * @return The principal name for ACLs
     */
    public static String toPrincipal(String clientID) {
        return "User:" + clientID;
    }

    /**
     * Allow the principal to perform the operation on all resources of the given resource type
     *
     * @param api          KafkaInstanceApi
     * @param principal    The principal id like a service account client id
     * @param resourceType The resource type for which the operation will be allowed
     * @param operation    The operation that will be allowed
     */
    public static void createAllowAnyACL(KafkaInstanceApi api, String principal, AclResourceType resourceType, AclOperation operation)
        throws ApiGenericException {

        var acl = new AclBinding()
            .principal(principal)
            .resourceType(resourceType)
            .patternType(AclPatternType.LITERAL)
            .resourceName("*")
            .permission(AclPermissionType.ALLOW)
            .operation(operation);

        log.debug(acl);

        api.createAcl(acl);
    }

    public static void createReadAnyTopicACL(KafkaInstanceApi api, String principal) throws ApiGenericException {
        log.debug("create read any topic ACL for principal '{}'", principal);
        createAllowAnyACL(api, principal, AclResourceType.TOPIC, AclOperation.READ);
    }

    public static void createWriteAnyTopicACL(KafkaInstanceApi api, String principal) throws ApiGenericException {
        log.debug("create write any topic ACL for principal '{}'", principal);
        createAllowAnyACL(api, principal, AclResourceType.TOPIC, AclOperation.WRITE);
    }

    public static void createReadAnyGroupACL(KafkaInstanceApi api, String principal) throws ApiGenericException {
        log.debug("create read any group ACL for principal '{}'", principal);
        createAllowAnyACL(api, principal, AclResourceType.GROUP, AclOperation.READ);
    }

    /**
     * Allow the principal to consume and produce messages from and to any topics and in any group.
     *
     * @param api       KafkaInstanceApi
     * @param principal The principal id like a service account client id
     */
    public static void createProducerAndConsumerACLs(KafkaInstanceApi api, String principal) throws ApiGenericException {
        createReadAnyGroupACL(api, principal);
        createReadAnyTopicACL(api, principal);
        createWriteAnyTopicACL(api, principal);
    }

    /**
     * Allow the Service account all operations on Topics, Transactions, Groups. In other words grant all ACLs for service account
     *
     * @param api       KafkaInstanceApi
     * @param serviceAccount The serviceAccount instance to be granted permissions
     */
    public static void applyAllowAllACLsOnResources(KafkaInstanceApi api, ServiceAccount serviceAccount, List<AclResourceType> resources) throws ApiGenericException {
        var principal = toPrincipal(serviceAccount.getClientId());

        // because ACLs that already exist are simply not created again, we do not need to check if the permission already exist.
        for (AclResourceType resourceType : resources) {
            createAllowAnyACL(api, principal, resourceType, AclOperation.ALL);
        }
    }

    public static boolean hasAllowAnyACL(List<AclBinding> acls, String principal, AclResourceType resourceType, AclOperation operation) {
        return acls.stream().anyMatch(a -> principal.equals(a.getPrincipal())
            && resourceType.equals(a.getResourceType())
            && AclPatternType.LITERAL.equals(a.getPatternType())
            && "*".equals(a.getResourceName())
            && AclPermissionType.ALLOW.equals(a.getPermission())
            && operation.equals(a.getOperation())
        );
    }

    public static boolean hasReadAnyTopicACL(List<AclBinding> acls, String principal) {
        return hasAllowAnyACL(acls, principal, AclResourceType.TOPIC, AclOperation.READ);
    }

    public static boolean hasWriteAnyTopicACL(List<AclBinding> acls, String principal) {
        return hasAllowAnyACL(acls, principal, AclResourceType.TOPIC, AclOperation.WRITE);
    }

    public static boolean hasReadAnyGroupACL(List<AclBinding> acls, String principal) {
        return hasAllowAnyACL(acls, principal, AclResourceType.GROUP, AclOperation.READ);
    }

    public static void applyProducerAndConsumerACLs(KafkaInstanceApi api, String principal) throws ApiGenericException {
        var aclPage = api.getAcls(null, null, null, principal, null, null, null, null, null, null);
        var acls = aclPage.getItems();

        if (!hasReadAnyGroupACL(acls, principal)) {
            createReadAnyGroupACL(api, principal);
        }

        if (!hasReadAnyTopicACL(acls, principal)) {
            createReadAnyTopicACL(api, principal);
        }

        if (!hasWriteAnyTopicACL(acls, principal)) {
            createWriteAnyTopicACL(api, principal);
        }
    }
}
