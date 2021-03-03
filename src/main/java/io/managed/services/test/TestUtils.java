package io.managed.services.test;

import com.ea.async.Async;
import io.managed.services.test.client.kafka.KafkaUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.common.KafkaFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.util.ExceptionUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/**
 * Test utils contains static help methods
 */
public class TestUtils {
    private static final Logger LOGGER = LogManager.getLogger(TestUtils.class);


    /**
     * Wait until the passed async lambda function return true
     *
     * @param vertx       Vertex
     * @param description A description used for logging and errors
     * @param interval    Interval between each call
     * @param timeout     Max time to wait before failing if the async lambda doesn't return true
     * @param isReady     The async lambda that will be call on each interval
     * @return A Future that will be completed once the lambda function returns true
     */
    public static <T> Future<T> waitFor(
            Vertx vertx,
            String description,
            Duration interval,
            Duration timeout,
            IsReady<T> isReady) {

        // generate the exception earlier to print a cleaner stacktrace in case of timeout
        Exception e = new Exception(String.format("timeout after %s waiting for %s", timeout.toString(), description));

        Instant deadline = Instant.now().plus(timeout);
        return waitFor(vertx, description, interval, deadline, e, isReady);
    }

    static <T> Future<T> waitFor(
            Vertx vertx,
            String description,
            Duration interval,
            Instant deadline,
            Exception timeout,
            IsReady<T> isReady) {

        boolean last = Instant.now().isAfter(deadline);

        LOGGER.info("waiting for {}; left={}", description, Duration.between(Instant.now(), deadline));
        return isReady.apply(last)
                .compose(r -> {
                    if (r.getValue0()) {
                        return Future.succeededFuture(r.getValue1());
                    }

                    // if the last request after the timeout didn't succeed fail with the timeout error
                    if (last) {
                        LOGGER.error(ExceptionUtils.readStackTrace(timeout));
                        return Future.failedFuture(timeout);
                    }

                    return sleep(vertx, interval)
                            .compose(v -> waitFor(vertx, description, interval, deadline, timeout, isReady));
                });
    }

    /**
     * Sync an async request by waiting for the passed Future do be completed
     *
     * @param future KafkaFuture
     * @param <T>    The Future result Type
     * @param <F>    Future
     * @return The Future result
     */
    public static <T, F extends KafkaFuture<T>> T await(F future) {
        return Async.await(KafkaUtils.toCompletionStage(future));
    }

    /**
     * Sync an async request by waiting for the passed Future do be completed
     *
     * @param future Future
     * @param <T>    The Future result Type
     * @param <F>    Future
     * @return The Future result
     */
    public static <T, F extends Future<T>> T await(F future) {
        return Async.await(future.toCompletionStage());
    }

    /**
     * Sync an async request by waiting for the passed Future do be completed
     *
     * @param future Future
     * @param <T>    The Future result Type
     * @param <F>    Future
     * @return The Future result
     */
    public static <T, F extends CompletableFuture<T>> T await(F future) {
        return Async.await(future);
    }

    /**
     * Convert a Java CompletionStage or CompletableFuture to a Vertx Future
     *
     * @param completion CompletionStage | CompletableFuture
     * @param <T>        Type
     * @return Vertx Future
     */
    public static <T> Future<T> toVertxFuture(CompletionStage<T> completion) {
        Promise<T> promise = Promise.promise();
        completion.whenComplete((r, t) -> {
            if (t == null) {
                promise.complete(r);
            } else {
                promise.fail(t);
            }
        });
        return promise.future();
    }

    /**
     * Similar to Iterable.forEach but it will wait for the Future returned by the action to complete before processing
     * the next item and return on the first Error.
     *
     * @param iterator Iterator
     * @param action   Lambda
     * @param <T>      T
     * @return a completed future once the forEach complete
     */
    public static <T> Future<Void> forEach(Iterator<T> iterator, Function<T, Future<Void>> action) {
        if (!iterator.hasNext()) {
            return Future.succeededFuture();
        }

        return action.apply(iterator.next())
                .compose(r -> forEach(iterator, action));
    }


    /**
     * Return a Future that will be completed after the passed duration.
     *
     * <pre>{@code
     * import static io.managed.services.test.TestUtils.await;
     * import static io.managed.services.test.TestUtils.waitFor;
     * import static java.time.Duration.ofSeconds;
     *
     * await(sleep(vertx, ofSeconds(10)))
     * }</pre>
     *
     * @param x Vertx
     * @param d Duration
     * @return Future
     */
    public static Future<Void> sleep(Vertx x, Duration d) {
        Promise<Void> p = Promise.promise();
        x.setTimer(d.toMillis(), l -> p.complete());
        return p.future();
    }

    public static long waitFor(String description, long pollIntervalMs, long timeoutMs, BooleanSupplier ready) {
        return waitFor(description, pollIntervalMs, timeoutMs, ready, () -> {
        });
    }

    /**
     * Wait for specific lambda expression
     *
     * @param description    description for logging
     * @param pollIntervalMs poll interval in ms
     * @param timeoutMs      timeout in ms
     * @param ready          lambda method for waiting
     * @param onTimeout      lambda method which is called when timeout is reached
     * @return Long
     */
    public static long waitFor(String description, long pollIntervalMs, long timeoutMs, BooleanSupplier ready, Runnable onTimeout) {
        LOGGER.debug("Waiting for {}", description);
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (true) {
            boolean result;
            try {
                result = ready.getAsBoolean();
            } catch (Exception e) {
                result = false;
            }
            long timeLeft = deadline - System.currentTimeMillis();
            if (result) {
                return timeLeft;
            }
            if (timeLeft <= 0) {
                onTimeout.run();
                WaitException waitException = new WaitException("Timeout after " + timeoutMs + " ms waiting for " + description);
                waitException.printStackTrace();
                throw waitException;
            }
            long sleepTime = Math.min(pollIntervalMs, timeLeft);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("{} not ready, will try again in {} ms ({}ms till timeout)", description, sleepTime, timeLeft);
            }
            try {
                //noinspection BusyWait
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                return deadline - System.currentTimeMillis();
            }
        }
    }

    public static File downloadYamlAndReplaceNamespace(String url, String namespace) throws IOException {
        File yamlFile = File.createTempFile("temp-file", ".yaml");

        try (InputStream bais = (InputStream) URI.create(url).toURL().openConnection().getContent();
             BufferedReader br = new BufferedReader(new InputStreamReader(bais, StandardCharsets.UTF_8));
             OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(yamlFile), StandardCharsets.UTF_8)) {

            StringBuilder sb = new StringBuilder();

            String read;
            while ((read = br.readLine()) != null) {
                sb.append(read);
                sb.append("\n");
            }
            String yaml = sb.toString();
            yaml = yaml.replaceAll("namespace: .*", "namespace: " + namespace);
            yaml = yaml.replace("securityContext:\n" +
                    "        runAsNonRoot: true\n" +
                    "        runAsUser: 65534", "");
            osw.write(yaml);
            return yamlFile;

        } catch (RuntimeException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static File replaceStringInYaml(String pathToOrigin, String originalns, String namespace) throws IOException {
        byte[] encoded;
        File yamlFile = File.createTempFile("temp-file", ".yaml");

        try (OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(yamlFile), StandardCharsets.UTF_8)) {
            encoded = Files.readAllBytes(Paths.get(pathToOrigin));

            String yaml = new String(encoded, StandardCharsets.UTF_8);
            yaml = yaml.replaceAll(originalns, namespace);

            osw.write(yaml);
            return yamlFile.toPath().toFile();
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Path getLogPath(String folderName, ExtensionContext context) {
        String testMethod = context.getDisplayName();
        Class<?> testClass = context.getTestClass().orElseThrow();
        return getLogPath(folderName, testClass, testMethod);
    }

    public static Path getLogPath(String folderName, TestInfo info) {
        String testMethod = info.getDisplayName();
        Class<?> testClass = info.getTestClass().orElseThrow();
        return getLogPath(folderName, testClass, testMethod);
    }

    public static Path getLogPath(String folderName, Class<?> testClass, String testMethod) {
        Path path = Environment.LOG_DIR.resolve(Paths.get(folderName, testClass.getName()));
        if (testMethod != null) {
            path = path.resolve(testMethod.replace("(", "").replace(")", ""));
        }
        return path;
    }

    public static void logWithSeparator(String pattern, String text) {
        LOGGER.info("=======================================================================");
        LOGGER.info(pattern, text);
        LOGGER.info("=======================================================================");
    }
}
