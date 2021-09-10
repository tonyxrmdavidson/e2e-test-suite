package io.managed.services.test.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.SneakyThrows;

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

    @SneakyThrows
    public static <T> T stdoutAsJson(Process process, Class<T> c) {
        return new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .readValue(stdout(process), c);
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
     * Read everything that is available in the stream right now without waiting for an EOF
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
}
