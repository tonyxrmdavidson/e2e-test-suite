package io.managed.services.test.apiclients;

import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import static java.net.HttpURLConnection.HTTP_OK;

/**
 * Abstract class for api clients which defines response handlers
 */
public abstract class ApiClient implements AutoCloseable {
    private static final Logger LOGGER = LogManager.getLogger(ApiClient.class);

    protected final Vertx vertx;
    protected final URL endpoint;
    protected final String apiVersion;

    private WebClient client;

    protected abstract String apiClientName();

    protected ApiClient(URL endpoint, final String apiVersion) {
        VertxOptions options = new VertxOptions()
                .setWorkerPoolSize(1)
                .setInternalBlockingPoolSize(1)
                .setEventLoopPoolSize(1);
        this.vertx = Vertx.vertx(options);
        this.endpoint = endpoint;
        this.apiVersion = apiVersion;
    }

    protected void reconnect() {
        closeClient();
        getClient();
    }

    /**
     * Get the client, create a new one if necessary.
     *
     * @return The client to use.
     */
    protected WebClient getClient() {
        if (this.client == null) {
            this.client = createClient();
        }
        return this.client;
    }

    @Override
    public void close() {
        closeClient();
        this.vertx.close();
    }

    private void closeClient() {
        if (client != null) {
            this.client.close();
            this.client = null;
        }
    }

    protected WebClient createClient() {
        return WebClient.create(vertx, new WebClientOptions()
                .setSsl(true)
                .setTrustAll(true)
                .setVerifyHost(false));
    }

    protected <T> void responseHandler(AsyncResult<HttpResponse<T>> ar, CompletableFuture<T> promise, int expectedCode,
                                       String warnMessage) {
        responseHandler(ar, promise, expectedCode, warnMessage, true);
    }

    protected <T> void responseHandler(AsyncResult<HttpResponse<T>> ar, CompletableFuture<T> promise, int expectedCode,
                                       String warnMessage, boolean throwException) {
        responseHandler(ar, promise, responseCode -> responseCode == expectedCode, String.valueOf(expectedCode), warnMessage, throwException);
    }

    protected <T> void responseHandler(AsyncResult<HttpResponse<T>> ar, CompletableFuture<T> promise, Predicate<Integer> expectedCodePredicate,
                                       String warnMessage, boolean throwException) {

        responseHandler(ar, promise, expectedCodePredicate, expectedCodePredicate.toString(), warnMessage, throwException);

    }

    /**
     * Response handler for vert.x api call
     * @param ar response from vert.x call
     * @param promise completable future
     * @param expectedCodePredicate expected HTTP code predicate
     * @param expectedCodeOrCodes expected http return codes
     * @param warnMessage logger warn message
     * @param throwException if false method does not throw exception
     */
    protected <T> void responseHandler(AsyncResult<HttpResponse<T>> ar, CompletableFuture<T> promise, Predicate<Integer> expectedCodePredicate,
                                       String expectedCodeOrCodes, String warnMessage, boolean throwException) {
        try {
            if (ar.succeeded()) {
                HttpResponse<T> response = ar.result();
                T body = response.body();
                if (expectedCodePredicate.negate().test(response.statusCode())) {
                    LOGGER.error("expected-code: {}, response-code: {}, body: {}, op: {}", expectedCodeOrCodes, response.statusCode(), response.body(), warnMessage);
                    promise.completeExceptionally(new RuntimeException("Status " + response.statusCode() + " body: " + (body != null ? body.toString() : null)));
                } else if (response.statusCode() < HTTP_OK || response.statusCode() >= HttpURLConnection.HTTP_MULT_CHOICE) {
                    if (throwException) {
                        promise.completeExceptionally(new RuntimeException(body == null ? "null" : body.toString()));
                    } else {
                        promise.complete(ar.result().body());
                    }
                } else {
                    promise.complete(ar.result().body());
                }
            } else {
                LOGGER.warn("Request failed: {}", warnMessage, ar.cause());
                promise.completeExceptionally(ar.cause());
            }
        } catch (io.vertx.core.json.DecodeException decEx) {
            if (ar.result().bodyAsString().toLowerCase(Locale.ENGLISH).contains("application is not available")) {
                LOGGER.warn("'{}' is not available.", apiClientName(), ar.cause());
                throw new IllegalStateException(String.format("'%s' is not available.", apiClientName()));
            } else {
                LOGGER.warn("Unexpected object received", ar.cause());
                throw new IllegalStateException("JsonObject expected, but following object was received: " + ar.result().bodyAsString());
            }
        }
    }

}
