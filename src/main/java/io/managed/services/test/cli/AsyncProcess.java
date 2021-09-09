package io.managed.services.test.cli;

import java.io.BufferedReader;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.managed.services.test.cli.ProcessUtils.buffer;
import static lombok.Lombok.sneakyThrow;

class AsyncProcess {

    private final Process process;
    private final ProcessHandle.Info info;

    AsyncProcess(Process process) {
        this.info = process.info();
        this.process = process;
    }

    public CompletableFuture<Process> future(Duration timeout) {
        var cause = new Exception();
        return process.onExit()

            // set a timeout for the process
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)

            // handle the process result
            .handle((p, t) -> {

                if (process.isAlive()) {
                    process.destroyForcibly();
                }

                if (t != null) {
                    throw sneakyThrow(t);
                }

                if (p.exitValue() != 0) {
                    throw sneakyThrow(new ProcessException(p, info, cause));
                }
                return p;
            });
    }

    public BufferedReader stdout() {
        return buffer(process.getInputStream());
    }

    public BufferedReader stderr() {
        return buffer(process.getErrorStream());
    }
}
