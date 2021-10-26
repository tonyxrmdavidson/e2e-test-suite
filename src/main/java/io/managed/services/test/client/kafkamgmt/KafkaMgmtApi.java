package io.managed.services.test.client.kafkamgmt;

import com.openshift.cloud.api.kas.DefaultApi;
import com.openshift.cloud.api.kas.invoker.ApiClient;
import com.openshift.cloud.api.kas.invoker.ApiException;
import com.openshift.cloud.api.kas.invoker.auth.HttpBearerAuth;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.KafkaRequestList;
import com.openshift.cloud.api.kas.models.KafkaRequestPayload;
import com.openshift.cloud.api.kas.models.MetricsInstantQueryList;
import io.managed.services.test.client.BaseApi;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiUnknownException;
import io.managed.services.test.client.oauth.KeycloakUser;
import lombok.extern.log4j.Log4j2;

import java.util.List;
import java.util.Objects;

@Log4j2
public class KafkaMgmtApi extends BaseApi<ApiException> {

    private final ApiClient apiClient;
    private final DefaultApi api;

    public KafkaMgmtApi(ApiClient apiClient, KeycloakUser user) {
        super(user);
        this.apiClient = Objects.requireNonNull(apiClient);
        this.api = new DefaultApi(apiClient);
    }

    @Override
    protected ApiUnknownException toApiException(Exception e) {
        if (e instanceof ApiException) {
            var ex = (ApiException) e;
            return new ApiUnknownException(ex.getMessage(), ex.getCode(), ex.getResponseHeaders(), ex.getResponseBody(), ex);
        }
        return null;
    }

    @Override
    protected void setAccessToken(String t) {
        ((HttpBearerAuth) apiClient.getAuthentication("Bearer")).setBearerToken(t);
    }

    public KafkaRequest getKafkaById(String id) throws ApiGenericException {
        return retry(() -> api.getKafkaById(id));
    }

    public KafkaRequestList getKafkas(String page, String size, String orderBy, String search) throws ApiGenericException {
        return retry(() -> api.getKafkas(page, size, orderBy, search));
    }

    public KafkaRequest createKafka(Boolean async, KafkaRequestPayload kafkaRequestPayload) throws ApiGenericException {
        return retry(() -> api.createKafka(async, kafkaRequestPayload));
    }

    public void deleteKafkaById(String id, Boolean async) throws ApiGenericException {
        // TODO: why does it return Error
        retry(() -> api.deleteKafkaById(id, async));
    }

    public MetricsInstantQueryList getMetricsByInstantQuery(String id, List<String> filters) throws ApiGenericException {
        return retry(() -> api.getMetricsByInstantQuery(id, filters));
    }
}
