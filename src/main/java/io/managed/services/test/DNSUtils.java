package io.managed.services.test;

import io.managed.services.test.cli.AsyncProcess;
import io.managed.services.test.cli.ProcessException;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;

public class DNSUtils {

    public static String dig(String hostname) {
        return dig(hostname, null);
    }

    public static String dig(String hostname, String server) {
        var cmd = new ArrayList<String>();
        cmd.add("dig");
        if (server != null) {
            cmd.add(String.format("@%s", server));
        }
        cmd.add(hostname);

        var processBuilder = new ProcessBuilder().command(cmd);

        Process process;
        try {
            process = processBuilder.start();
        } catch (IOException e) {
            return e.getMessage();
        }

        var asyncProcess = new AsyncProcess(process);
        try {
            return asyncProcess.sync(Duration.ofMinutes(1)).stdoutAsString();
        } catch (ProcessException e) {
            return e.getMessage();
        }
    }
}
