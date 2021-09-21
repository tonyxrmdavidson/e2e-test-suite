package io.managed.services.test.client.github;

import io.managed.services.test.client.BaseVertxClient;
import io.managed.services.test.client.exception.ResponseException;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;

import java.net.HttpURLConnection;
import java.net.URI;
import java.util.List;
import java.util.Locale;

import static org.slf4j.helpers.MessageFormatter.format;

public class GitHub extends BaseVertxClient {

    final static String GITHUB_URL = "https://api.github.com";

    public GitHub(Vertx vertx) {
        super(vertx, options());
    }

    static WebClientOptions options() {
        return optionsForURI(URI.create(GITHUB_URL))
            .setFollowRedirects(false);
    }

    private Future<List<Release>> getReleases(String org, String repo) {
        String path = String.format("/repos/%s/%s/releases", org, repo);

        return client.get(path)
            .send()
            .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
            .map(r -> r.bodyAsJson(Release[].class))
            .map(r -> List.of(r));
    }

    public Future<Release> getLatestRelease(String org, String repo) {
        return this.getReleases(org, repo)
            // get the first non-draft release
            .map(releases -> releases.stream().filter(r -> !r.getDraft()).findFirst())
            .compose(o -> o
                .map(r -> Future.succeededFuture(r))
                .orElseGet(() -> Future.failedFuture(format("latest release not found in repository: {}/{}", org, repo).getMessage())));
    }

    public Future<Release> getReleaseByTagName(String org, String repo, String name) {
        if (name.toLowerCase(Locale.ROOT).equals("latest")) {
            return getLatestRelease(org, repo);
        }

        var path = String.format("/repos/%s/%s/releases/tags/%s", org, repo, name);
        return client.get(path)
            .send()
            .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
            .map(r -> r.bodyAsJson(Release.class));
    }

    public Future<Void> downloadAsset(String org, String repo, String id, WriteStream<Buffer> stream) {
        return client.get(String.format("/repos/%s/%s/releases/assets/%s", org, repo, id))
            .putHeader("Accept", "application/octet-stream")
            .send()
            .compose(r -> assertResponse(r, HttpURLConnection.HTTP_MOVED_TEMP))
            .compose(r -> {
                var l = r.getHeader("Location");
                if (l == null) {
                    return Future.failedFuture(new ResponseException("Location header not found", r));
                }
                return Future.succeededFuture(l);
            })
            .compose(l -> client.getAbs(l).as(BodyCodec.pipe(stream)).send())
            .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
            .map(r -> r.bodyAsJson(Void.class));
    }
}
