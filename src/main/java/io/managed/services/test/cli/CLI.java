package io.managed.services.test.cli;

import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class CLI {
    private static final Logger LOGGER = LogManager.getLogger(CLI.class);

    private final String workdir;
    private final String cmd;

    public CLI(String workdir, String name) {
        this.workdir = workdir;
        this.cmd = String.format("./%s", name);
    }

    private ProcessBuilder builder(String... command) {
        var cmd = new ArrayList<String>();
        cmd.add(this.cmd);
        cmd.addAll(Arrays.asList(command));

        return new ProcessBuilder(cmd)
            .directory(new File(workdir));
    }

    private Future<Process> exec(String... command) {
        return execAsync(command).compose(a -> a.future());
    }

    private Future<AsyncProcess> execAsync(String... command) {
        try {
            return Future.succeededFuture(new AsyncProcess(builder(command).start()));
        } catch (IOException e) {
            return Future.failedFuture(e);
        }
    }

    /**
     * This method only starts the CLI login, use CLIUtils.login instead of this method
     * to login using username and password
     */
    public Future<AsyncProcess> login() {
        return execAsync("login", "--print-sso-url");
    }

    public Future<Process> logout() {
        return exec("logout");
    }

    public Future<Process> listKafkas() {
        return exec("kafka", "list");
    }
}
