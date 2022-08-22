package io.managed.services.test.prometheuswebclient;

import okhttp3.OkHttpClient;

import java.util.HashMap;
import java.util.Map;

public class PrometheusWebClientBuilder {
    private String baseUrl;
    private String urlResourcePath;
    private final Map<String, String> headersMap = new HashMap<>();
    private final OkHttpClient httpClient = new OkHttpClient();


    public PrometheusWebClientBuilder withBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
        return this;
    }

    public PrometheusWebClientBuilder withUrlResourcePath(String urlResourcePath) {
        this.urlResourcePath = urlResourcePath;
        return this;
    }

    public PrometheusWebClientBuilder addHeaderEntry(String key, String value) {
        this.headersMap.put(key, value);
        return this;
    }

    public PrometheusWebClient build() {
        return new PrometheusWebClient(
                this.baseUrl,
                this.urlResourcePath,
                this.headersMap,
                this.httpClient
        );
    }
}
