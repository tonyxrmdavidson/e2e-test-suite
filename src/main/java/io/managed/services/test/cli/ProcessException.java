package io.managed.services.test.cli;


public class ProcessException extends Exception {
    public final String commandLine;
    public final int exitCode;
    public final String stderr;
    public final String stdout;

    public ProcessException(String commandLine, int exitCode, String stdout, String stderr, String output, Exception cause) {
        super(message(commandLine, exitCode, output), cause);
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

    public static String message(String commandLine, int exitCode, String output) {
        return String.format("cmd '%s' failed with exit code: %d\n", commandLine, exitCode)
            + output + "\n";
    }
}
