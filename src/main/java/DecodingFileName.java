//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.example;

public class DecodingFileName {
    public DecodingFileName() {
    }

    public static String fileNameToUrl(String fileName) {
        fileName = fileName.replaceAll("__________\\^", "|");
        fileName = fileName.replaceAll("_________\\^", "<");
        fileName = fileName.replaceAll("________\\^", ">");
        fileName = fileName.replaceAll("_______\\^", "\"");
        fileName = fileName.replaceAll("______\\^", "\\?");
        fileName = fileName.replaceAll("_____\\^", "\\*");
        fileName = fileName.replaceAll("____\\^", ":");
        fileName = fileName.replaceAll("___\\^", "/");
        fileName = fileName.replaceAll("__\\^", "\\\\");
        return fileName.replaceAll("\\.html$", "");
    }
}
