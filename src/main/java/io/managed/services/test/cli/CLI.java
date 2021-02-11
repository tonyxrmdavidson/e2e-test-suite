package io.managed.services.test.cli;

import io.vertx.core.Vertx;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

public class CLI {

    private final Vertx vertx;
    private final String workdir;
    private final String cmd;

    public CLI(Vertx vertx, String workdir, String name) {
        this.vertx = vertx;
        this.workdir = workdir;
        this.cmd = String.format("./%s", name);
    }

    ProcessBuilder builder(String... command) {
        var cmd = new ArrayList<String>();
        cmd.add(this.cmd);
        cmd.addAll(Arrays.asList(command));

        return new ProcessBuilder(cmd)
            .directory(new File(workdir));
    }

}
