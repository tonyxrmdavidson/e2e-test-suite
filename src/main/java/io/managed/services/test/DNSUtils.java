package io.managed.services.test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class DNSUtils {

    public static String dnsInfo(String hostname) {

        var apiKey = Environment.WHOISXMLAPI_KEY;
        if (apiKey.isBlank()) {
            return "Error: WHOISXMLAPI_KEY not set";
        }

        var client = HttpClient.newHttpClient();

        var uri = String.format("https://www.whoisxmlapi.com/whoisserver/DNSService?apiKey=%s&domainName=%s&type=_all", apiKey, hostname);

        var request = HttpRequest.newBuilder(
                URI.create(uri))
            .header("accept", "application/json")
            .build();

        try {
            return client.send(request, HttpResponse.BodyHandlers.ofString()).body();
        } catch (IOException | InterruptedException e) {
            throw new Error(e);
        }
    }
}
