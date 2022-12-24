package common.io.LocalFS;

import java.io.File;
import java.util.Objects;

public class FileSystem {
    public static Boolean deleteFile(File file) {
        if (file == null || !file.exists()) {
            return false;
        }
        for (File f : Objects.requireNonNull(file.listFiles())) {
            if (f.isDirectory()) {
                deleteFile(f);
            } else {
                f.delete();
            }
        }
        file.delete();
        return true;
    }
}
