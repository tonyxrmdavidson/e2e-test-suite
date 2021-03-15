package io.managed.services.test.cli;

import io.managed.services.test.TestUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class ProcessUtils {

    /**
     * Read the whole stdout from the Process
     *
     * @param process Process
     * @return stdout
     */
    static String stdout(Process process) {
        return read(process.getInputStream());
    }

    static <T> T stdoutAsJson(Process process, Class<T> c) {
        return TestUtils.asJson(c, stdout(process));
    }

    /**
     * Read the whole stderr from the Process
     *
     * @param process Process
     * @return stderr
     */
    static String stderr(Process process) {
        return read(process.getErrorStream());
    }

    private static String read(InputStream stream) {
        return buffer(stream).lines().collect(Collectors.joining());
    }

    static BufferedReader buffer(InputStream stream) {
        return new BufferedReader(new InputStreamReader(stream));
    }

    static String toError(Process process) {
        return toError(process, process.info());
    }

    static String toError(Process process, ProcessHandle.Info info) {
        return String.format("cmd '%s' failed with exit code: %d\n", info.commandLine().orElse(null), process.exitValue())
                + "-- STDOUT --\n"
                + stdout(process)
                + "\n"
                + "-- STDERR --\n"
                + stderr(process)
                + "\n"
                + "-- END --\n";
    }
}
