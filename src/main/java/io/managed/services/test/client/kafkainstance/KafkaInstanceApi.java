package io.managed.services.test.client.kafkainstance;

import com.openshift.cloud.api.kas.auth.AclsApi;
import com.openshift.cloud.api.kas.auth.GroupsApi;
import com.openshift.cloud.api.kas.auth.TopicsApi;
import com.openshift.cloud.api.kas.auth.invoker.ApiClient;
import com.openshift.cloud.api.kas.auth.invoker.ApiException;
import com.openshift.cloud.api.kas.auth.models.ConsumerGroup;
import com.openshift.cloud.api.kas.auth.models.ConsumerGroupList;
import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.Topic;
import com.openshift.cloud.api.kas.auth.models.TopicsList;
import io.managed.services.test.client.BaseApi;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiUnknownException;

public class KafkaInstanceApi extends BaseApi<ApiException> {

    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final AclsApi aclsApi;
    private final GroupsApi groupsApi;
    private final TopicsApi topicsApi;

    public KafkaInstanceApi(ApiClient apiClient) {
        this(new AclsApi(apiClient), new GroupsApi(apiClient), new TopicsApi(apiClient));
    }

    public KafkaInstanceApi(AclsApi aclsApi, GroupsApi groupsApi, TopicsApi topicsApi) {
        super(ApiException.class);
        this.aclsApi = aclsApi;
        this.groupsApi = groupsApi;
        this.topicsApi = topicsApi;
    }

    @Override
    protected ApiUnknownException toApiException(ApiException e) {
        return new ApiUnknownException(e.getMessage(), e.getCode(), e.getResponseHeaders(), e.getResponseBody(), e);
    }

    public TopicsList getTopics() throws ApiGenericException {
        return getTopics(null, null, null, null, null);
    }

    public TopicsList getTopics(Integer size, Integer page, String filter, String order, String orderKey) throws ApiGenericException {
        return handle(() -> topicsApi.getTopics(null, null, size, filter, page, order, orderKey));
    }

    public Topic getTopic(String topicName) throws ApiGenericException {
        return handle(() -> topicsApi.getTopic(topicName));
    }

    public Topic createTopic(NewTopicInput newTopicInput) throws ApiGenericException {
        return handle(() -> topicsApi.createTopic(newTopicInput));
    }

    public void deleteTopic(String topicName) throws ApiGenericException {
        vhandle(() -> topicsApi.deleteTopic(topicName));
    }

    public ConsumerGroupList getConsumerGroups() throws ApiGenericException {
        return getConsumerGroups(null, null, null, null, null, null);
    }

    public ConsumerGroupList getConsumerGroups(Integer size, Integer page, String topic, String groupIdFilter, String order, String orderKey) throws ApiGenericException {
        return handle(() -> groupsApi.getConsumerGroups(null, null, size, page, topic, groupIdFilter, order, orderKey));
    }

    public ConsumerGroup getConsumerGroupById(String consumerGroupId) throws ApiGenericException {
        return getConsumerGroupById(consumerGroupId, null, null, null, null);
    }

    public ConsumerGroup getConsumerGroupById(String consumerGroupId, String order, String orderKey, Integer partitionFilter, String topic) throws ApiGenericException {
        return handle(() -> groupsApi.getConsumerGroupById(consumerGroupId, order, orderKey, partitionFilter, topic));
    }

    public void deleteConsumerGroupById(String consumerGroupId) throws ApiGenericException {
        vhandle(() -> groupsApi.deleteConsumerGroupById(consumerGroupId));
    }
}
