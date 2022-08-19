package io.managed.services.test.billing;

import io.managed.services.test.TestBase;
import io.managed.services.test.prometheuswebclient.PrometheusException;
import io.managed.services.test.prometheuswebclient.PrometheusBasedWebClient;
import lombok.extern.log4j.Log4j2;
import org.testng.annotations.Test;

import java.io.IOException;

@Log4j2
public class SandTest extends TestBase {

    //private final static String TOKEN_REFRESHER_URL = "http://localhost:8085";
    //private final String queryApiPrefix = "/api/metrics/v1/managedkafka/api/v1/query";
    //private

    @Test
    void whenGetRequest_thenCorrect() throws IOException, PrometheusException {
        //OkHttpClient httpClient = new OkHttpClient();
        //
        ////httpClient
        //var token = "sha256~UD0E1qDe7UO-PAtjnd_ZgBiJp2fWYIfgm8Y1Zyx4eKc";
        //var baseUrl = "https://kafka-prometheus-managed-application-services-observability.apps.mk-stage-0622.bd59.p1.openshiftapps.com";
        //// _id="cbt4ead730qd5itpjrmg"
        //var resourceAndQuery = "/api/v1/query?query=kafka_id:haproxy_server_bytes_in_total:rate1h_gibibytes{_id=\"cbt4ead730qd5itpjrmg\"}";
        //
        //Request request = new Request.Builder()
        //        .addHeader("Authorization", "Bearer " + token)
        //        .url(baseUrl + resourceAndQuery)
        //        .build();
        //
        //Call call = httpClient.newCall(request);
        //Response response = call.execute();
        //Assert.assertEquals(response.code(), 200);

        var metric = "kafka_id:haproxy_server_bytes_out_total:rate1h_gibibytes";

        PrometheusBasedWebClient pc = new PrometheusBasedWebClient();
        PrometheusBasedWebClient.Query query = new PrometheusBasedWebClient.Query();
        //query.metric(metric).label("_id", kafka.getId());
        query.metric(metric).aggregateFunction("sum");
        //query.metric(metric);
        var result = pc.query(query);
        log.info(result.data.result.get(0).doubleValue());
        //snapshotValues.put(metric, );
    }
}
