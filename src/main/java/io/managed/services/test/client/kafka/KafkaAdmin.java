package io.managed.services.test.client.kafka;

import io.vertx.core.Future;
import org.apache.kafka.clients.admin.Admin;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ElectionType;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.token.delegation.DelegationToken;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.managed.services.test.client.kafka.KafkaUtils.toVertxFuture;
public class KafkaAdmin {

    public final Admin admin;

    // TODO: This parameters shouldn't be passed top down from the test
    // TODO: We shouldn't relay on the `__strimzi_canary`
    static final String STRIMZI_TOPIC = "__strimzi_canary";
    static final String STRIMZI_CANARY_GROUP = "strimzi-canary-group";
    static final String GROUP_ID = "new-group-id";

    public KafkaAdmin(String bootstrapHost, String clientID, String clientSecret) {
        Map<String, Object> conf = KafkaAuthMethod.oAuthConfigs(bootstrapHost, clientID, clientSecret)
                .entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        admin = Admin.create(conf);
    }

    public Future<Void> createTopic(String name) {
        return createTopic(name, null, null);
    }

    public Future<Void> createTopic(String name, Integer partitions, Short replicas) {
        NewTopic topic = new NewTopic(name, Optional.ofNullable(partitions), Optional.ofNullable(replicas));
        return toVertxFuture(admin.createTopics(Collections.singleton(topic)).all());
    }

    public Future<Set<String>> listTopics() {
        return toVertxFuture(admin.listTopics().names());
    }

    public Future<Map<String, TopicDescription>> getMapOfTopicNameAndDescriptionByName(String name) {
        return toVertxFuture(admin.describeTopics(Collections.singleton(name)).all());
    }

    public Future<Void> deleteTopic(String name) {
        return toVertxFuture(admin.deleteTopics(Collections.singleton(name)).all());
    }

    public Future<Void> addAclResource(ResourceType resourceType) {
        ResourcePattern pattern = new ResourcePattern(resourceType, "foo", PatternType.LITERAL);
        AccessControlEntry entry = new AccessControlEntry("*", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW);
        AclBinding aclBinding  = new AclBinding(pattern, entry);
        List<AclBinding> listOfAclBindings = new ArrayList<>();
        listOfAclBindings.add(aclBinding);
        return toVertxFuture(admin.createAcls(listOfAclBindings).all());
    }

    public Future<Collection<AclBinding>> listAclResource(ResourceType resourceType) {
        ResourcePatternFilter myFilter = new ResourcePatternFilter(resourceType, "foo", PatternType.ANY);
        AccessControlEntryFilter myFilter2 = new AccessControlEntryFilter("*", "*", AclOperation.ANY, AclPermissionType.ALLOW);
        AclBindingFilter totalFilter = new AclBindingFilter(myFilter, myFilter2);
        return toVertxFuture(admin.describeAcls(totalFilter).values());
    }

    public Future<Map<ConfigResource, Config>> getConfigurationBroker(String brokerId) {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, brokerId);
        List<ConfigResource> configResourceAsList = Collections.singletonList(configResource);
        return toVertxFuture(admin.describeConfigs(configResourceAsList).all());
    }

    public Future<Map<ConfigResource, Config>> getConfigurationTopic(String topicName) {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        List<ConfigResource> configResourceAsList = Collections.singletonList(configResource);
        return toVertxFuture(admin.describeConfigs(configResourceAsList).all());
    }

    public Future<Map<ConfigResource, Config>> getConfigurationBrokerLogger(String brokerLoggerId) {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER_LOGGER, brokerLoggerId);
        List<ConfigResource> configResourceAsList = Collections.singletonList(configResource);
        return toVertxFuture(admin.describeConfigs(configResourceAsList).all());
    }

    public Future<Map<ClientQuotaEntity, Map<String, Double>>> getConfigurationUserAll() {
        ClientQuotaFilterComponent clientQuotaFilterComponent = ClientQuotaFilterComponent.ofDefaultEntity("user");
        List<ClientQuotaFilterComponent> clientQuotaFilterComponents = Collections.singletonList(clientQuotaFilterComponent);
        ClientQuotaFilter clientQuotaFilter2 = ClientQuotaFilter.contains(clientQuotaFilterComponents);
        return toVertxFuture(admin.describeClientQuotas(clientQuotaFilter2).entities());
    }

    public Future<Void> configureBrokerResource(ConfigResource.Type resourceType, AlterConfigOp.OpType configurationType, String resourceName) {
        ConfigResource configResource = new ConfigResource(resourceType, resourceName);
        ConfigEntry configEntry = new ConfigEntry("client-id", "someValue2");
        AlterConfigOp op = new AlterConfigOp(configEntry, configurationType);
        Map map = new HashMap<ConfigResource, List<AlterConfigOp>>() {
            {
                put(configResource, Collections.singletonList(op));
            }
        };
        return toVertxFuture(admin.incrementalAlterConfigs(map).all());
    }


    public Future<Void> alterConfigurationUser() {
        Map<String, String> map = new HashMap<>();
        map.put("client-id", "client-id");
        ClientQuotaEntity clientQuotaEntity = new ClientQuotaEntity(map);
        ClientQuotaAlteration.Op cop = new ClientQuotaAlteration.Op("partitions", 1.0);

        ClientQuotaAlteration clientQuotaAlteration = new ClientQuotaAlteration(clientQuotaEntity, Collections.singletonList(cop));
        return toVertxFuture(admin.alterClientQuotas(Collections.singletonList(clientQuotaAlteration)).all());
    }

    public Future<Collection<ConsumerGroupListing>> listConsumerGroups() {
        return toVertxFuture(admin.listConsumerGroups().all());
    }

    public Future<Map<String, ConsumerGroupDescription>> describeConsumerGroups() {
        List<String> listOfIds = Collections.singletonList(STRIMZI_CANARY_GROUP);
        return toVertxFuture(admin.describeConsumerGroups(listOfIds).all());
    }

    public Future<Void> deleteConsumerGroups() {
        List<String> listOfIds = Collections.singletonList(STRIMZI_CANARY_GROUP);
        return toVertxFuture(admin.deleteConsumerGroups(listOfIds).all());
    }

    public Future<Void> resetOffsets() {
        TopicPartition topicPartition = new TopicPartition(STRIMZI_TOPIC, 0);
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(0, "foo");
        Map map = Map.of(topicPartition, offsetAndMetadata);
        return toVertxFuture(admin.alterConsumerGroupOffsets(GROUP_ID, map).all());
    }

    public Future<Void> deleteOffset() {
        TopicPartition topicPartition = new TopicPartition(STRIMZI_TOPIC, 0);
        return  toVertxFuture(admin.deleteConsumerGroupOffsets(GROUP_ID, Set.of(topicPartition)).all());
    }

    public Future<Void> deleteRecords() {
        TopicPartition topicPartition = new TopicPartition(STRIMZI_TOPIC, 0);
        RecordsToDelete recordsToDelete = RecordsToDelete.beforeOffset(0);
        Map map = new HashMap<TopicPartition, RecordsToDelete>() {
            {
                put(topicPartition, recordsToDelete);
            }
        };
        return toVertxFuture(admin.deleteRecords(map).all());
    }


    public Future<Void> electLeader(ElectionType electionType) {
        TopicPartition topicPartition = new TopicPartition(STRIMZI_TOPIC, 2);
        return toVertxFuture(admin.electLeaders(electionType, Set.of(topicPartition)).all());
    }

    public Future<Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>>> logDirs() {
        return toVertxFuture(admin.describeLogDirs(List.of(1)).all());
    }

    public Future<Void> reassignPartitions() {
        TopicPartition topicPartition = new TopicPartition(STRIMZI_TOPIC, 0);
        NewPartitionReassignment newPartitionReassignment = new NewPartitionReassignment(List.of(0));
        Map map = new HashMap<TopicPartition, Optional<NewPartitionReassignment>>() {
            {
                put(topicPartition, Optional.of(newPartitionReassignment));
            }
        };
        return toVertxFuture(admin.alterPartitionReassignments(map).all());
    }

    public Future<DelegationToken> createDelegationToken() {
        return toVertxFuture(admin.createDelegationToken().delegationToken());
    }

    public Future<List<DelegationToken>> describeDelegationToken() {
        return toVertxFuture(admin.describeDelegationToken().delegationTokens());
    }
    public void close() {
        admin.close(Duration.ofSeconds(3));
    }
}


