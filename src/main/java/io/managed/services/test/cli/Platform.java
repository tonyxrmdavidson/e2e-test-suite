package io.managed.services.test.cli;

import java.util.Locale;

public enum Platform {
    WINDOWS,
    LINUX,
    MAC;


    public static Platform getArch() {
        String os = System.getProperty("os.name").toLowerCase(Locale.ENGLISH);
        boolean isWin = os.contains("win");
        boolean isMac = os.contains("mac");
        boolean isUnix = os.contains("nix") || os.contains("nux") || os.contains("aix");

        if (isUnix) {
            return LINUX;
        } else if (isMac) {
            return MAC;
        } else if (isWin) {
            return WINDOWS;
        }
        return LINUX;
    }

    @Override
    public String toString() {
        switch (this) {
            case WINDOWS:
                return "windows";
            case LINUX:
                return "linux";
            case MAC:
                return "macOS";
            default:
                throw new IllegalArgumentException();
        }
    }
}
