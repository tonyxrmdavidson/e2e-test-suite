package io.managed.services.test;

import io.managed.services.test.cli.ProcessException;
import io.managed.services.test.cli.ProcessUtils;

import java.io.IOException;
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

        var processInfo = process.info();

        int exitCode;
        try {
            exitCode = process.waitFor();
        } catch (InterruptedException e) {
            return e.getMessage();
        }

        if (exitCode != 0) {
            return new ProcessException(process, processInfo, null).getMessage();
        }
        return ProcessUtils.readNow(process.getInputStream());
    }
}
