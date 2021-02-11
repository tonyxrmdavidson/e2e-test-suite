package io.managed.services.test.cli;

import io.managed.services.test.Environment;
import io.managed.services.test.client.BaseVertxClient;
import io.managed.services.test.client.oauth.KeycloakOAuthUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientSession;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

public class CLIUtils {
    private static final Logger LOGGER = LogManager.getLogger(CLIUtils.class);

    public static void extractCLI(String archive, String entry, String dest) throws IOException {

        var bi = new BufferedInputStream(Files.newInputStream(Paths.get(archive)));
        var gzi = new GzipCompressorInputStream(bi);
        var tar = new TarArchiveInputStream(gzi);

        ArchiveEntry e;
        while ((e = tar.getNextEntry()) != null) {
            if (!e.getName().equals(entry)) {
                continue;
            }

            LOGGER.info("extract {} to {}", e.getName(), dest);

            if (!tar.canReadEntryData(e)) {
                throw new IOException("can not read entry " + e.getName());
            }

            File f = new File(dest);
            if (e.isDirectory()) {
                throw new IOException("the entry " + e.getName() + " is a directory");
            } else {
                File parent = f.getParentFile();
                if (!parent.isDirectory() && !parent.mkdirs()) {
                    throw new IOException("failed to create directory " + parent);
                }
                try (OutputStream o = Files.newOutputStream(f.toPath())) {
                    IOUtils.copy(tar, o);
                    return;
                }
            }
        }
        throw new IOException("cli not found");
    }

    public static Future<Void> login(Vertx vertx, CLI cli) {

        Process process;
        try {
            process = cli.builder("login", "--print-sso-url")
                .start();
        } catch (IOException e) {
            return Future.failedFuture(e);
        }

        WebClient client = WebClient.create(vertx);
        WebClientSession session = WebClientSession.create(client);

        return parseSSOUrl(vertx, process.getInputStream())
            .compose(l -> KeycloakOAuthUtils.startLogin(session, l))
            .compose(r -> KeycloakOAuthUtils.postUsernamePassword(session, r, Environment.SSO_USERNAME, Environment.SSO_PASSWORD))
            .compose(r -> BaseVertxClient.assertResponse(r, 302))
            .compose(r -> BaseVertxClient.followRedirect(session, r))
            .compose(r -> BaseVertxClient.assertResponse(r, 200))
            .map((HttpResponse<Buffer> r) -> {
                LOGGER.info(r.bodyAsString());
                return null;
            });
    }

    private static Future<String> parseSSOUrl(Vertx vertx, InputStream stdout) {
        var is = new InputStreamReader(stdout);
        var br = new BufferedReader(is);

        return vertx.executeBlocking(h -> {
            var full = new ArrayList<String>();
            while (true) {
                try {
                    var l = br.readLine();
                    if (l == null) {
                        h.fail(new Exception("SSO URL Not found\n-- STDOUT --\n" + String.join("\n", full)));
                        return;
                    }

                    if (l.matches("https://sso.redhat.com/auth/.*")) {
                        h.complete(l);
                        return;
                    }
                    full.add(l);

                } catch (Exception e) {
                    h.fail(e);
                    return;
                }
            }
        });
    }

}
