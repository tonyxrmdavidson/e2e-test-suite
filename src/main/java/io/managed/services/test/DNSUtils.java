package io.managed.services.test;

import io.managed.services.test.cli.ProcessUtils;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;

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

    public static String dig(String hostname) {
        return dig(hostname, null);
    }

    public static String dig(String hostname, String server) {
        var cmd = new ArrayList<String>();
        cmd.add("dig");
        cmd.add("hostname");
        if (server != null) {
            cmd.add(server);
        }

        var processBuilder = new ProcessBuilder().command(cmd);

        Process process;
        try {
            process = processBuilder.start();
        } catch (IOException e) {
            return e.getMessage();
        }

        var processInfo = process.info();

        int exitCode;
        try {
            exitCode = process.waitFor();
        } catch (InterruptedException e) {
            return e.getMessage();
        }

        if (exitCode != 0) {
            return ProcessUtils.toError(process, processInfo);
        }
        return ProcessUtils.readNow(process.getInputStream());
    }
}
