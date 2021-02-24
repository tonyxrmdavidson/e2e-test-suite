package io.managed.services.test.client.github;

import io.managed.services.test.client.BaseVertxClient;
import io.managed.services.test.TestUtils;
import io.managed.services.test.client.ResponseException;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;

import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Locale;

public class GitHub extends BaseVertxClient {

    final static String GITHUB_URL = "https://api.github.com";

    final TokenCredentials token;

    public GitHub(Vertx vertx, String token) {
        super(vertx, options());
        this.token = new TokenCredentials(token);
    }

    static WebClientOptions options() {
        return optionsForURI(URI.create(GITHUB_URL))
                .setFollowRedirects(false);
    }

    private Future<Release[]> getReleases(String org, String repo) {
        String path = String.format("/repos/%s/%s/releases", org, repo);

        return client.get(path)
            .authentication(token)
            .send()
            .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
            .map(r -> r.bodyAsJson(Release[].class));
    }

    public Future<Release> getReleaseByTagName(String org, String repo, String name) {
        String path;
        if (name.toLowerCase(Locale.ROOT).equals("latest")) {
            var releases = TestUtils.await(this.getReleases(org, repo));

            for (Release release : releases) {
                // we don't want to test it if it is a draft release
                if (release.draft) {
                    continue;
                }
                // get the first non-draft release
                name = release.tagName;
                break;
            }
        }

        path = String.format("/repos/%s/%s/releases/tags/%s", org, repo, name);

        return client.get(path)
                .authentication(token)
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK))
                .map(r -> r.bodyAsJson(Release.class));
    }

    public Future<Void> downloadAsset(String org, String repo, String id, WriteStream<Buffer> stream) {
        return client.get(String.format("/repos/%s/%s/releases/assets/%s", org, repo, id))
                .authentication(token)
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
