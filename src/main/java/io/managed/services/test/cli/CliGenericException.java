package io.managed.services.test.cli;


import java.net.HttpURLConnection;
import java.util.regex.Pattern;

public class CliGenericException extends Exception {

    public CliGenericException(ProcessException e) {
        super(e);
    }

    public static CliGenericException exception(ProcessException e) {
        switch (parseCode(e.getStderr())) {
            case HttpURLConnection.HTTP_NOT_FOUND:
                return new CliNotFoundException(e);
            case HttpURLConnection.HTTP_UNAUTHORIZED:
            case HttpURLConnection.HTTP_FORBIDDEN:
            case 429:
            case HttpURLConnection.HTTP_CONFLICT:
            case 423:
            default:
                return new CliGenericException(e);
        }
    }

    /**
     * Try to parse for the stderr the last HTTP error code
     *
     * @param stderr Process stderr
     * @return Last HTTP error code
     */
    public static int parseCode(String stderr) {
        var reg = Pattern.compile("^HTTP/1\\.1 (\\d{3}) .*$");
        var lines = stderr.split("\\r?\\n");
        for (int i = lines.length - 1; i >= 0; i--) {
            var line = lines[i];
            var match = reg.matcher(line);
            if (match.matches()) {
                return Integer.parseInt(match.group(1));
            }
        }
        return 0;
    }
}
