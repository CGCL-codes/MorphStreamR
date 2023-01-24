package common.io.Compressor;


import org.xerial.snappy.Snappy;

import java.io.IOException;

public class SnappyCompressor implements Compressor{

    @Override
    public String compress(String in) {
        try {
            return new String(Snappy.compress(in));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String uncompress(String in) {
        byte[] dataBytes = in.getBytes();
        try {
            return new String(Snappy.uncompress(dataBytes));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
