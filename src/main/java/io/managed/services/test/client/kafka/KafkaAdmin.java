package io.managed.services.test.client.kafka;

import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.KafkaFuture;
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
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class KafkaAdmin implements AutoCloseable {

    public final Admin admin;

    public KafkaAdmin(String bootstrapHost, String clientID, String clientSecret) {
        this(bootstrapHost, KafkaAuthMethod.oAuthConfigs(bootstrapHost, clientID, clientSecret));
    }

    public KafkaAdmin(String bootstrapHost, String token) {
        this(bootstrapHost, KafkaAuthMethod.oAuthTokenConfigs(bootstrapHost, token));
    }

    public KafkaAdmin(String bootstrapHost, Map<String, String> config) {
        Map<String, Object> conf = config.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        admin = Admin.create(conf);
    }

    @SneakyThrows
    private <T> T get(KafkaFuture<T> f) {
        try {
            return f.get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    public void createTopic(String name) {
        createTopic(name, null, null);
    }

    public void createTopic(String name, Integer partitions, Short replicas) {
        NewTopic topic = new NewTopic(name, Optional.ofNullable(partitions), Optional.ofNullable(replicas));
        get(admin.createTopics(Collections.singleton(topic)).all());
    }

    public Set<String> listTopics() {
        return get(admin.listTopics().names());
    }

    public void deleteTopic(String name) {
        get(admin.deleteTopics(Collections.singleton(name)).all());
    }

    public void addAclResource(ResourceType resourceType) {
        ResourcePattern pattern = new ResourcePattern(resourceType, "foo", PatternType.LITERAL);
        AccessControlEntry entry = new AccessControlEntry("*", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW);
        AclBinding aclBinding = new AclBinding(pattern, entry);
        List<AclBinding> listOfAclBindings = new ArrayList<>();
        listOfAclBindings.add(aclBinding);
        get(admin.createAcls(listOfAclBindings).all());
    }

    public Collection<AclBinding> listAclResource(ResourceType resourceType) {
        ResourcePatternFilter myFilter = new ResourcePatternFilter(resourceType, "foo", PatternType.ANY);
        AccessControlEntryFilter myFilter2 = new AccessControlEntryFilter("*", "*", AclOperation.ANY, AclPermissionType.ALLOW);
        AclBindingFilter totalFilter = new AclBindingFilter(myFilter, myFilter2);
        return get(admin.describeAcls(totalFilter).values());
    }

    public void deleteAclResource(ResourceType resourceType) {
        ResourcePatternFilter myFilter = new ResourcePatternFilter(resourceType, "foo", PatternType.ANY);
        AccessControlEntryFilter myFilter2 = new AccessControlEntryFilter("*", "*", AclOperation.ANY, AclPermissionType.ALLOW);
        AclBindingFilter totalFilter = new AclBindingFilter(myFilter, myFilter2);
        get(admin.deleteAcls(Collections.singleton(totalFilter)).all());
    }

    public void getConfigurationBroker(String brokerId) {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, brokerId);
        List<ConfigResource> configResourceAsList = Collections.singletonList(configResource);
        get(admin.describeConfigs(configResourceAsList).all());
    }

    public Map<ConfigResource, Config> getConfigurationTopic(String topicName) {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        List<ConfigResource> configResourceAsList = Collections.singletonList(configResource);
        return get(admin.describeConfigs(configResourceAsList).all());
    }

    public void getConfigurationBrokerLogger(String brokerLoggerId) {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER_LOGGER, brokerLoggerId);
        List<ConfigResource> configResourceAsList = Collections.singletonList(configResource);
        get(admin.describeConfigs(configResourceAsList).all());
    }

    public Map<ClientQuotaEntity, Map<String, Double>> getConfigurationUserAll() {
        ClientQuotaFilterComponent clientQuotaFilterComponent = ClientQuotaFilterComponent.ofDefaultEntity("user");
        List<ClientQuotaFilterComponent> clientQuotaFilterComponents = Collections.singletonList(clientQuotaFilterComponent);
        ClientQuotaFilter clientQuotaFilter2 = ClientQuotaFilter.contains(clientQuotaFilterComponents);
        return get(admin.describeClientQuotas(clientQuotaFilter2).entities());
    }

    public void configureBrokerResource(ConfigResource.Type resourceType, AlterConfigOp.OpType configurationType, String resourceName) {
        ConfigResource configResource = new ConfigResource(resourceType, resourceName);
        ConfigEntry configEntry = new ConfigEntry("client-id", "someValue2");
        AlterConfigOp op = new AlterConfigOp(configEntry, configurationType);
        var map = new HashMap<ConfigResource, Collection<AlterConfigOp>>() {
            {
                put(configResource, Collections.singletonList(op));
            }
        };
        get(admin.incrementalAlterConfigs(map).all());
    }


    public void alterConfigurationUser() {
        Map<String, String> map = new HashMap<>();
        map.put("client-id", "client-id");
        ClientQuotaEntity clientQuotaEntity = new ClientQuotaEntity(map);
        ClientQuotaAlteration.Op cop = new ClientQuotaAlteration.Op("partitions", 1.0);

        ClientQuotaAlteration clientQuotaAlteration = new ClientQuotaAlteration(clientQuotaEntity, Collections.singletonList(cop));
        get(admin.alterClientQuotas(Collections.singletonList(clientQuotaAlteration)).all());
    }

    public Collection<ConsumerGroupListing> listConsumerGroups() {
        return get(admin.listConsumerGroups().all());
    }

    public Map<String, ConsumerGroupDescription> describeConsumerGroups(String groupID) {
        List<String> listOfIds = Collections.singletonList(groupID);
        return get(admin.describeConsumerGroups(listOfIds).all());
    }

    public void deleteConsumerGroups(String groupID) {
        List<String> listOfIds = Collections.singletonList(groupID);
        get(admin.deleteConsumerGroups(listOfIds).all());
    }

    public void resetOffsets(String topicName, String groupID) {
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(0, "foo");
        var map = Map.of(topicPartition, offsetAndMetadata);
        get(admin.alterConsumerGroupOffsets(groupID, map).all());
    }

    public void deleteOffset(String topicName, String groupID) {
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
        get(admin.deleteConsumerGroupOffsets(groupID, Set.of(topicPartition)).all());
    }

    public void deleteRecords(String topicName) {
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
        RecordsToDelete recordsToDelete = RecordsToDelete.beforeOffset(0);
        var map = new HashMap<TopicPartition, RecordsToDelete>() {
            {
                put(topicPartition, recordsToDelete);
            }
        };
        get(admin.deleteRecords(map).all());
    }


    public void electLeader(ElectionType electionType, String topicName) {
        TopicPartition topicPartition = new TopicPartition(topicName, 2);
        get(admin.electLeaders(electionType, Set.of(topicPartition)).all());
    }

    public void logDirs() {
        get(admin.describeLogDirs(List.of(1)).allDescriptions());
    }

    public void reassignPartitions(String topicName) {
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
        NewPartitionReassignment newPartitionReassignment = new NewPartitionReassignment(List.of(0));
        var map = new HashMap<TopicPartition, Optional<NewPartitionReassignment>>() {
            {
                put(topicPartition, Optional.of(newPartitionReassignment));
            }
        };
        get(admin.alterPartitionReassignments(map).all());
    }

    public void createDelegationToken() {
        get(admin.createDelegationToken().delegationToken());
    }

    public void describeDelegationToken() {
        get(admin.describeDelegationToken().delegationTokens());
    }

    public void createAcls(Collection<AclBinding> acls) {
        get(admin.createAcls(acls).all());
    }

    @Override
    public void close() {
        admin.close();
    }
}


