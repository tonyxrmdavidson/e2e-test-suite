package io.managed.services.test.cli;


public class ProcessException extends Exception {
    public final String commandLine;
    public final int exitCode;
    public final String stderr;
    public final String stdout;

    public ProcessException(Process process, ProcessHandle.Info info, Exception cause) {
        this(info.commandLine().orElse(null),
            process.exitValue(),
            ProcessUtils.readNow(process.getInputStream()),
            ProcessUtils.readNow(process.getErrorStream()),
            cause);
    }

    ProcessException(String commandLine, int exitCode, String stdout, String stderr, Exception cause) {
        super(message(commandLine, exitCode, stdout, stderr), cause);
        this.commandLine = commandLine;
        this.exitCode = exitCode;
        this.stdout = stdout;
        this.stderr = stderr;
    }

    @SuppressWarnings("unused")
    public String getCommandLine() {
        return commandLine;
    }

    @SuppressWarnings("unused")
    public int getExitCode() {
        return exitCode;
    }

    @SuppressWarnings("unused")
    public String getStdout() {
        return stdout;
    }

    public String getStderr() {
        return stderr;
    }

    public static String message(String commandLine, int exitCode, String stdout, String stderr) {
        return String.format("cmd '%s' failed with exit code: %d\n", commandLine, exitCode)
            + "-- STDOUT --\n"
            + stdout
            + "\n"
            + "-- STDERR --\n"
            + stderr
            + "\n"
            + "-- END --\n";
    }
}
