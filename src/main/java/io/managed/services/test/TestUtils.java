package io.managed.services.test;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.codec.impl.BodyCodecImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.MessageFactory2;
import org.apache.logging.log4j.message.ParameterizedMessageFactory;
import org.testng.ITestContext;
import org.testng.SkipException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

/**
 * Test utils contains static help methods
 */
public class TestUtils {
    private static final Logger LOGGER = LogManager.getLogger(TestUtils.class);

    private static final long MINUTES = 60 * 1000;
    private static final long DEFAULT_TIMEOUT = 3 * MINUTES;

    private static final MessageFactory2 MESSAGE_FACTORY = new ParameterizedMessageFactory();

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
                    return Future.failedFuture(timeout);
                }

                return sleep(vertx, interval)
                    .compose(v -> waitFor(vertx, description, interval, deadline, timeout, isReady));
            });
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

    public static <T> Future<Void> forEach(Iterable<T> iterable, Function<T, Future<Void>> action) {
        return forEach(iterable.iterator(), action);
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

    /**
     * Format the message like log4j
     *
     * @param message String format
     * @param params  Objects
     * @return String
     */
    public static String message(String message, Object... params) {
        return MESSAGE_FACTORY.newMessage(message, params).getFormattedMessage();
    }

    public static Path getLogPath(String folderName, ITestContext context) {
        String testMethod = context.getName();
        Class<?> testClass = context.getClass();
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

    public static <T> T asJson(Class<T> c, String s) {
        return BodyCodecImpl.jsonDecoder(c).apply(Buffer.buffer(s));
    }

    public static String decodeBase64(String encodedString) {
        return new String(Base64.getDecoder().decode(encodedString));
    }

    /**
     * Block the Thread and wait for for the Future result and in case of Future failure throw the Future error
     */
    public static <T> T bwait(Future<T> future) throws Throwable {
        if (Vertx.currentContext() != null) {
            throw new IllegalCallerException("bwait() can not be used in within a Vert.x callback");
        }

        // await for the future to complete
        var latch = new CountDownLatch(1);
        future.onComplete(__ -> latch.countDown());
        latch.await();

        // assert the future result
        if (future.failed()) {
            throw future.cause();
        }
        return future.result();
    }

    public static void assumeTeardown() {
        if (Environment.SKIP_TEARDOWN) {
            throw new SkipException("skip teardown");
        }
    }
}
