package io.managed.services.test.cli;

import io.managed.services.test.TestUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.stream.Collectors;

public class ProcessUtils {

    /**
     * Read the whole stdout from the Process
     *
     * @param process Process
     * @return stdout
     */
    public static String stdout(Process process) {
        return read(process.getInputStream());
    }

    public static <T> T stdoutAsJson(Process process, Class<T> c) {
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

    /**
     * Read everything that is available in the stream right now without waiting for a EOF
     */
    public static String readNow(InputStream stream) {
        try {
            return new String(stream.readNBytes(stream.available()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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

    static String toTimeoutError(Process process, ProcessHandle.Info info) {
        return toError(String.format("timeout cmd '%s'", info.commandLine().orElse(null)), process);
    }

    public static String toError(Process process, ProcessHandle.Info info) {
        return toError(String.format("cmd '%s' failed with exit code: %d", info.commandLine().orElse(null), process.exitValue()), process);
    }

    static String toError(String message, Process process) {
        return String.format("%s\n", message)
            + "-- STDOUT --\n"
            + readNow(process.getInputStream())
            + "\n"
            + "-- STDERR --\n"
            + readNow(process.getErrorStream())
            + "\n"
            + "-- END --\n";
    }
}
