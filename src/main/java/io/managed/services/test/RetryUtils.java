package io.managed.services.test;

import io.prometheus.client.Counter;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;

@Log4j2
public class RetryUtils {
    private static final int DEFAULT_THRESHOLD = 5;

    private static final Counter ERRORS = Counter.build()
        .name("test_skipped_errors")
        .labelNames("launch", "class", "method", "exception", "message")
        .help("Test retry errors counter.").register();

    public static <T> Future<T> retry(
        Vertx x,
        int backtrace,
        Supplier<Future<T>> call,
        Function<Throwable, Boolean> condition) {

        return retry(x, backtrace, call, condition, DEFAULT_THRESHOLD);
    }

    /**
     * Retry the call supplier if the supplier returns a failed future and
     * the condition returns true.
     *
     * @param x         Vertx
     * @param backtrace This method will automatically try to assign the function and file of the retried function when
     *                  reporting int to prometheus, to do it in the most precise way the caller function can tell this
     *                  method how much going backward in the stacktrace to find the function that should be reported
     *                  <p>
     *                  Ex: KeycloakOAuth.login() -> KeycloakOAuth.retry(backtrace: 1) -> RetryUtils.retry() = KeycloakOAuth.login()
     * @param call      The supplier to call the first time and retry in case of failure
     * @param condition The condition to retry the call supplier
     * @param attempts  The max number of attempts before returning the last failure
     * @param <T>       T
     * @return Future
     */
    public static <T> Future<T> retry(
        Vertx x,
        int backtrace,
        Supplier<Future<T>> call,
        Function<Throwable, Boolean> condition,
        int attempts) {

        return retry(x, caller(backtrace), call, condition, attempts, Duration.ofSeconds(1));
    }

    private static <T> Future<T> retry(
        Vertx x,
        StackWalker.StackFrame caller,
        Supplier<Future<T>> call,
        Function<Throwable, Boolean> condition,
        int attempts,
        Duration interval) {

        Function<Throwable, Future<T>> retry = t -> {
            logSkip(caller, t);

            // retry the API call
            return sleep(x, interval)
                .compose(r -> retry(x, caller, call, condition, attempts - 1, interval.plusSeconds(3)));
        };

        return call.get().recover(t -> {
            if (attempts > 0 && condition.apply(t)) {
                // retry the call if there are available attempts and if the condition returns true
                return retry.apply(t);
            }
            return Future.failedFuture(t);
        });
    }

    public static Future<Void> sleep(Vertx x, Duration d) {
        Promise<Void> p = Promise.promise();
        x.setTimer(d.toMillis(), l -> p.complete());
        return p.future();
    }

    public static <T, E extends Throwable> T retry(
        int backtrace,
        ThrowingSupplier<T, E> call,
        Function<Throwable, Boolean> condition)
        throws E {

        return retry(backtrace, call, condition, DEFAULT_THRESHOLD);
    }

    /**
     * Retry the call supplier if the supplier throws an exception and the condition returns true.
     *
     * @param backtrace This method will automatically try to assign the function and file of the retried function when
     *                  reporting int to prometheus, to do it in the most precise way the caller function can tell this
     *                  method how much going backward in the stacktrace to find the function that should be reported
     *                  <p>
     *                  Ex: KeycloakOAuth.login() -> KeycloakOAuth.retry(backtrace: 1) -> RetryUtils.retry() = KeycloakOAuth.login()
     * @param call      The supplier to call the first time and retry in case of failure
     * @param condition The condition to retry the call supplier
     * @param attempts  The max number of attempts before returning the last failure
     * @param <T>       T
     * @return Future
     */
    public static <T, E extends Throwable> T retry(
        int backtrace,
        ThrowingSupplier<T, E> call,
        Function<Throwable, Boolean> condition,
        int attempts)
        throws E {

        return retry(backtrace, null, call, condition, attempts, Duration.ofSeconds(1));
    }

    @SneakyThrows
    public static <T, E extends Throwable> T retry(
        int backtrace,
        StackWalker.StackFrame caller,
        ThrowingSupplier<T, E> call,
        Function<Throwable, Boolean> condition,
        int attempts,
        Duration interval)
        throws E {

        try {
            return call.get();

        } catch (Throwable t) {

            // retry the call if there are available attempts and if the condition returns true
            if (attempts > 0 && condition.apply(t)) {

                if (caller == null) caller = caller(backtrace);
                logSkip(caller, t);

                // retry the API call
                Thread.sleep(interval.toMillis());
                return retry(backtrace, caller, call, condition, attempts - 1, interval.plusSeconds(3));
            }

            // if the attempts are finished or the condition doesn't match throw the original exception
            throw t;
        }
    }

    private static void logSkip(StackWalker.StackFrame caller, Throwable t) {
        log.error("{}.{}(): skip error: ", caller.getClassName(), caller.getMethodName(), t);
        ERRORS.labels(Environment.LAUNCH_KEY, caller.getClassName(), caller.getMethodName(), t.getClass().getName(), t.getMessage()).inc();
    }

    private static StackWalker.StackFrame caller(int backtrace) {

        var walker = StackWalker.getInstance();
        return walker.walk(frames -> frames
                // ignore all the frames within this class
                .filter(f -> !f.getClassName().equals(RetryUtils.class.getName()))
                // skip n frames
                .skip(backtrace)
                .findFirst())
            .orElseThrow();
    }
}
