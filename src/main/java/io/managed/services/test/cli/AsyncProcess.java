package io.managed.services.test.cli;

import io.vertx.core.Future;

import java.io.BufferedReader;

import static io.managed.services.test.TestUtils.toVertxFuture;
import static io.managed.services.test.cli.ProcessUtils.buffer;
import static io.managed.services.test.cli.ProcessUtils.toError;

class AsyncProcess {

    private final Process process;
    private final ProcessHandle.Info info;

    AsyncProcess(Process process) {
        this.info = process.info();
        this.process = process;
    }

    public Future<Process> future() {
        return toVertxFuture(process.onExit())
                .compose(p -> {
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
