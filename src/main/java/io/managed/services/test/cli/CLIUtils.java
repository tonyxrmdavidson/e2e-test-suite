package io.managed.services.test.cli;

import io.managed.services.test.client.BaseVertxClient;
import io.managed.services.test.client.oauth.KeycloakOAuthUtils;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
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
import java.io.OutputStream;
import java.net.HttpURLConnection;
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

    public static Future<Void> login(Vertx vertx, CLI cli, String username, String password) {
        WebClient client = WebClient.create(vertx);
        WebClientSession session = WebClientSession.create(client);

        LOGGER.info("start CLI login with username: {}", username);
        return cli.login()
            .compose(process -> {

                LOGGER.info("start oauth login against CLI");
                var oauthFuture = parseSSOUrl(vertx, process.stdout())
                    .compose(l -> KeycloakOAuthUtils.startLogin(session, l))
                    .compose(r -> KeycloakOAuthUtils.postUsernamePassword(session, r, username, password))
                    .compose(r -> BaseVertxClient.assertResponse(r, HttpURLConnection.HTTP_MOVED_TEMP))
                    .compose(r -> BaseVertxClient.followRedirect(session, r))
                    .compose(r -> BaseVertxClient.assertResponse(r, HttpURLConnection.HTTP_OK))
                    .map(v -> {
                        LOGGER.info("oauth login completed");
                        return null;
                    });

                var cliFuture = process.future()
                    .map(r -> {
                        LOGGER.info("CLI login completed");
                        return null;
                    });

                return CompositeFuture.all(oauthFuture, cliFuture);
            })
            .map(n -> null);
    }

    private static Future<String> parseSSOUrl(Vertx vertx, BufferedReader stdout) {

        return vertx.executeBlocking(h -> {
            var full = new ArrayList<String>();
            while (true) {
                try {
                    var l = stdout.readLine();
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