package io.managed.services.test.cli;

import io.vertx.core.Future;
import org.apache.tika.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static io.managed.services.test.TestUtils.toVertxFuture;

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
                    try {
                        return Future.failedFuture(new ProcessException(toError(p), p));
                    } catch (IOException e) {
                        return Future.failedFuture(e);
                    }
                }
                return Future.succeededFuture(p);
            });
    }

    public BufferedReader stdout() {
        return new BufferedReader(new InputStreamReader(process.getInputStream()));
    }

    public BufferedReader stderr() {
        return new BufferedReader(new InputStreamReader(process.getErrorStream()));
    }

    private String toError(Process process) throws IOException {
        return String.format("cmd '%s' failed with exit code: %d\n", info.commandLine().orElse(null), process.exitValue())
            + "-- STDOUT --\n"
            + String.join("\n", IOUtils.readLines(stdout()))
            + "\n"
            + "-- STDERR --\n"
            + String.join("\n", IOUtils.readLines(stderr()))
            + "\n"
            + "-- END --\n";
    }
}
