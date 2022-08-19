package io.managed.services.test.prometheuswebclient;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

@Log4j2
public class PrometheusBasedWebClient {

    // TODO token needs to be obtained from service account associated with necessary rules
    private String token = "sha256~b4oOXM7viN-pO_Mc-BvQmwHqNwtFC7injMVq5yLRyI4";
    // TODO nase url of prometheus (can be obtained with oc client as route smt.)
    private String baseUrl = "https://kafka-prometheus-managed-application-services-observability.apps.mk-stage-0622.bd59.p1.openshiftapps.com";

    // TODO query prefix
    //private String resourceAndQuery = "/api/v1/query?query=";
    private String resourceAndQuery = "/api/v1/query";
    // TODO specific query
    private String specificQuery = "kafka_id:haproxy_server_bytes_in_total:rate1h_gibibytes{_id='cbt4ead730qd5itpjrmg'}";

    private Map<String,String> headersMap = new HashMap<>();

    private OkHttpClient httpClient = new OkHttpClient();



    public PrometheusBasedWebClient PrometheusBasedWebClient() {
    }

    public PrometheusBasedWebClient addHeader(String key, String value){
        this.headersMap.put(key, value);
        return this;
    }



    public QueryResult query(Query query) throws PrometheusException {
        return query(query.toString());
    }

    public QueryResult query(String query) throws PrometheusException {

        var builder = UriBuilder.fromPath(baseUrl);
        builder.path(resourceAndQuery);

        try {
            builder.queryParam("query", URLEncoder.encode(query, "ISO-8859-1"));
        } catch (UnsupportedEncodingException ex) {
            throw new PrometheusException(ex);
        }

        var uri = builder.build();
        Request request;
        try {
            request = new Request
                    .Builder()
                    .url(uri.toURL())
                    .addHeader("Authorization", "Bearer " + token)
                    .build();
        } catch (MalformedURLException ex) {
            throw new PrometheusException(ex);
        }


        try (var response = httpClient.newCall(request).execute()) {
            // all successful requests to Observatorium return 200
            if (response.code() != 200) {
                throw new PrometheusException(String.format("expected 200 but got %d", response.code()));
            }

            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(response.body().string(), QueryResult.class);
        } catch (IOException ex) {
            throw new PrometheusException(ex);
        }
    }

    public static class Query {
        private String metric;

        private List labels = new ArrayList<>();

        private Queue<String> aggregateFunctions = new LinkedList<>();

        public Query metric(String metric) {
            this.metric = metric;
            return this;
        }

        public Query label(String label, String value) {
            labels.add(String.format("%s='%s'", label, value));
            return this;
        }

        public Query aggregateFunction(String functionName) {
            aggregateFunctions.add(functionName);
            return this;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            // call all aggregation functions in order they were called in
            for (String elem : aggregateFunctions) {
                sb.append(String.format("%s(",elem));
            }
            sb.append(metric);
            sb.append("{");
            sb.append(String.join(",", labels));
            sb.append("}");
            // append ")" to mark end of function in promql
            sb.append(")".repeat(aggregateFunctions.size()));

            log.debug( "promql query: {}:",sb.toString());
            return sb.toString();
        }
    }
}
