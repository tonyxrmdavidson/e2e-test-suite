package io.managed.services.test.cli;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.io.BufferedReader;
import java.time.Duration;

import static io.managed.services.test.TestUtils.toVertxFuture;
import static io.managed.services.test.cli.ProcessUtils.buffer;
import static io.managed.services.test.cli.ProcessUtils.toError;
import static io.managed.services.test.cli.ProcessUtils.toTimeoutError;

class AsyncProcess {

    private final Vertx vertx;
    private final Process process;
    private final ProcessHandle.Info info;

    AsyncProcess(Vertx vertx, Process process) {
        this.vertx = vertx;
        this.info = process.info();
        this.process = process;
    }

    public Future<Process> future(Duration timeout) {
        var onExitFuture = toVertxFuture(process.onExit());

        var timeoutPromise = Promise.promise();
        var timeoutTimer = vertx.setTimer(timeout.toMillis(), __ -> {
            var m = toTimeoutError(process, info);
            process.destroy();
            timeoutPromise.fail(new ProcessException(m, process));
        });

        onExitFuture.onComplete(__ -> {
            vertx.cancelTimer(timeoutTimer);
            timeoutPromise.tryComplete();
        });

        return CompositeFuture.all(onExitFuture, timeoutPromise.future())
                .compose(__ -> {
                    var p = onExitFuture.result();
                    if (p.exitValue() != 0) {
                        return Future.failedFuture(new ProcessException(toError(p, info), p));
                    }
                    return Future.succeededFuture(p);
                });
    }

    public BufferedReader stdout() {
        return buffer(process.getInputStream());
    }

    public BufferedReader stderr() {
        return buffer(process.getErrorStream());
    }
}
