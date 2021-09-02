package io.managed.services.test.client.kafkamgmt;

import com.openshift.cloud.api.kas.DefaultApi;
import com.openshift.cloud.api.kas.invoker.ApiClient;
import com.openshift.cloud.api.kas.invoker.ApiException;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.KafkaRequestList;
import com.openshift.cloud.api.kas.models.KafkaRequestPayload;
import io.managed.services.test.client.BaseApi;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiUnknownException;

public class KafkaMgmtApi extends BaseApi<ApiException> {

    private final DefaultApi api;

    public KafkaMgmtApi(ApiClient apiClient) {
        this(new DefaultApi(apiClient));
    }

    public KafkaMgmtApi(DefaultApi defaultApi) {
        super(ApiException.class);
        this.api = defaultApi;
    }

    @Override
    protected ApiUnknownException toApiException(ApiException e) {
        return new ApiUnknownException(e.getMessage(), e.getCode(), e.getResponseHeaders(), e.getResponseBody(), e);
    }

    public KafkaRequest getKafkaById(String id) throws ApiGenericException {
        return handle(() -> api.getKafkaById(id));
    }

    public KafkaRequestList getKafkas(String page, String size, String orderBy, String search) throws ApiGenericException {
        return handle(() -> api.getKafkas(page, size, orderBy, search));
    }

    public KafkaRequest createKafka(Boolean async, KafkaRequestPayload kafkaRequestPayload) throws ApiGenericException {
        return handle(() -> api.createKafka(async, kafkaRequestPayload));
    }

    public void deleteKafkaById(String id, Boolean async) throws ApiGenericException {
        // TODO: why does it return Error
        handle(() -> api.deleteKafkaById(id, async));
    }
}
