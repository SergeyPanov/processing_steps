/**
 * Enumeration which maps colors from native language on the terminal's code.
 * Example: Red -> "\u001B[31m"
 */

package cz.vutbr.fit.knot.corpproc.queryserver;


public enum Color {
    Black("\u001B[30m"),
    Red("\u001B[31m"),
    Green("\u001B[32m"),
    Yellow("\u001B[33m"),
    Blue("\u001B[34m"),
    Magenta("\u001B[35m"),
    Cyan("\u001B[36m"),
    LightGray("\u001B[37m"),
    DarkGray("\u001B[90m"),
    LightRed("\u001B[91"),
    LightGreen("\u001B[32m"),
    LightYellow("\u001B[93m"),
    LightBlue("\u001B[94m"),
    LightMagenta("\u001B[95m"),
    LightCyan("\u001B[96m"),
    White("\u001B[97m"),
    Default("\u001B[39m");

    private String color;

    Color(String s) {
        color = s;
    }

    public String getColor() {
        return color;
    }
}
