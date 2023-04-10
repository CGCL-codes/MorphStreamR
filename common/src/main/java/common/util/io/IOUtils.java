package common.util.io;

import java.io.InputStream;
import java.util.Scanner;

public class IOUtils {
    public static String convertStreamToString(InputStream is) {
        Scanner s = null;
        try {
            s = new Scanner(is).useDelimiter("\\A");
        } catch (Exception ex) {
            System.out.println("Error!" + ex.getMessage());
        }
        return s.hasNext() ? s.next() : "";
    }

    public static String convertStreamToString(InputStream is, String charsetName) {
        Scanner s = new Scanner(is, charsetName).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }
    public static void println(String str) {
        StackTraceElement[] stackTrace = new Throwable().getStackTrace();
        System.out.println(str + " : ---" + stackTrace[stackTrace.length > 2 ? 1 : 0].toString());
    }
}
