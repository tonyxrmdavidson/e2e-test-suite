package io.managed.services.test.observatorium;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import javax.ws.rs.core.UriBuilder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URLEncoder;

@Log4j2
public class ObservatoriumClient {
    private final String tokenRefresherBaseUrl = "http://localhost:8085";

    private final String apiPrefix = "/api/metrics/v1/managedkafka/api/v1/query";

    private final String queryPrefix = "query";

    private String oidcIssureUrl = null;
    private String observatoriumUrl = null;
    private String clientSecret;
    private String clientId;

    private OkHttpClient httpClient = new OkHttpClient();

    private Process tokenRefresher = null;

    public ObservatoriumClient(String observatoriumUrl, String clientId, String clientSecret, String oidcIssureUrl) {
        this.oidcIssureUrl = oidcIssureUrl;
        this.observatoriumUrl = observatoriumUrl;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    public void start() throws ObservatoriumException {
        var pb = new ProcessBuilder();
        pb.command("./bin/token-refresher"
                , "--url", observatoriumUrl
                , "--oidc.issuer-url", oidcIssureUrl
                , "--oidc.client-id", clientId
                , "--oidc.client-secret", clientSecret
                , "--web.listen", ":8085"
        );

        try {
            tokenRefresher = pb.start();
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(tokenRefresher.getErrorStream()));

            String line;
            while ((line = reader.readLine()) != null) {
                log.info(line);

                // Need to wait until the token refresher is ready
                if (line.contains("starting internal HTTP server")) {
                    break;
                }
            }

            this.waitForReady();
        } catch (IOException ex) {
            throw new ObservatoriumException(ex);
        }
    }

    public void stop() {
        if (tokenRefresher != null) {
            tokenRefresher.destroy();
        }
    }

    public QueryResult query(String metric) throws ObservatoriumException {
        var builder = UriBuilder.fromPath(tokenRefresherBaseUrl);
        builder.path(apiPrefix);

        try {
            builder.queryParam(queryPrefix, URLEncoder.encode(metric, "ISO-8859-1"));
        } catch (UnsupportedEncodingException ex) {
            throw new ObservatoriumException(ex);
        }

        var uri = builder.build();

        Request request;
        try {
            request = new Request.Builder().url(uri.toURL()).build();
        } catch (MalformedURLException ex) {
            throw new ObservatoriumException(ex);
        }

        try (var response = httpClient.newCall(request).execute()) {
            if (response.code() != 200) {
                throw new ObservatoriumException(String.format("expected 200 but got %d", response.code()));
            }


            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(response.body().string(), QueryResult.class);
        } catch (IOException ex) {
            throw new ObservatoriumException(ex);
        }
    }

    private void waitForReady() throws ObservatoriumException {
        var request = new Request.Builder().url(tokenRefresherBaseUrl).build();
        try {
            var response = httpClient.newCall(request).execute();
            if (response.code() != 200) {
                throw new ObservatoriumException(String.format("expected 200 but got %d", response.code()));
            }
        } catch (IOException ex) {
            throw new ObservatoriumException(ex);
        }
    }
}
