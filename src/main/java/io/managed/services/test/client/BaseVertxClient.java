package io.managed.services.test.client;

import io.managed.services.test.Environment;
import io.managed.services.test.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.HttpURLConnection;
import java.net.URI;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.managed.services.test.TestUtils.sleep;
import static java.time.Duration.ofSeconds;

public abstract class BaseVertxClient {
    private static final Logger LOGGER = LogManager.getLogger(TestUtils.class);

    protected final Vertx vertx;
    protected final WebClient client;

    public BaseVertxClient(Vertx vertx, String uri) {
        this(vertx, URI.create(uri));
    }


    public BaseVertxClient(Vertx vertx, URI uri) {
        this(vertx, optionsForURI(uri));
    }

    public BaseVertxClient(Vertx vertx, WebClientOptions options) {
        this(vertx, WebClient.create(vertx, options));
    }

    public BaseVertxClient(Vertx vertx, WebClient client) {
        this.vertx = vertx;
        this.client = client;
    }

    protected static WebClientOptions optionsForURI(URI uri) {
        return new WebClientOptions()
                .setDefaultHost(uri.getHost())
                .setDefaultPort(getPort(uri))
                .setSsl(isSsl(uri))
                .setConnectTimeout((int) Environment.API_TIMEOUT_MS);
    }

    static Integer getPort(URI u) {
        if (u.getPort() == -1) {
            switch (u.getScheme()) {
                case "https":
                    return 443;
                case "http":
                    return 80;
            }
        }
        return u.getPort();
    }

    static Boolean isSsl(URI u) {
        switch (u.getScheme()) {
            case "https":
                return true;
            case "http":
                return false;
        }
        return false;
    }

    public <T> Future<T> retry(Supplier<Future<T>> call) {
        return retry(call, Environment.API_CALL_THRESHOLD);
    }

    public <T> Future<T> retry(Supplier<Future<T>> call, int attempts) {

        Function<Throwable, Future<T>> retry = t -> {
            LOGGER.error("skip error: ", t);

            // retry the API call
            return sleep(vertx, ofSeconds(1))
                    .compose(r -> retry(call, attempts - 1));
        };

        return call.get().recover(t -> {
            if (attempts <= 0) {
                // no more attempts remaining
                return Future.failedFuture(t);
            }

            if (t instanceof ResponseException) {
                if (((ResponseException) t).response.statusCode() == HttpURLConnection.HTTP_INTERNAL_ERROR) {
                    return retry.apply(t);
                }
            }

            if (t instanceof VertxException) {
                if (t.getMessage().equals("Connection was closed")) {
                    return retry.apply(t);
                }
            }

            return Future.failedFuture(t);
        });
    }

    public static <T> Future<HttpResponse<T>> assertResponse(HttpResponse<T> response, Integer statusCode) {
        if (response.statusCode() != statusCode) {
            String message = String.format("Expected status code %d but got %s", statusCode, response.statusCode());
            return Future.failedFuture(new ResponseException(message, response));
        }
        return Future.succeededFuture(response);
    }

    public static Future<String> getRedirectLocation(HttpResponse<Buffer> response) {
        var l = response.getHeader("Location");
        if (l == null) {
            return Future.failedFuture(new ResponseException("Location header not found", response));
        }
        return Future.succeededFuture(l);
    }

    public static Future<HttpResponse<Buffer>> followRedirect(WebClient client, HttpResponse<Buffer> response) {
        return getRedirectLocation(response)
                .compose(l -> client.getAbs(l).send());
    }
}
