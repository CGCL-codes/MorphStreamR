package common.io;

import common.io.Compressor.*;
import fi.iki.yak.ts.compression.gorilla.GorillaCompressor;
import fi.iki.yak.ts.compression.gorilla.LongArrayOutput;

import java.io.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

public class TestIO {
    public static void main(String[] args) throws Exception {
        long now = LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        LongArrayOutput output = new LongArrayOutput();
        GorillaCompressor c = new GorillaCompressor(now, output);
    }
    public void stringTest() throws IOException {
        String[] testStrings = new String[3];
        testStrings[0] = "This is a test String";
        testStrings[1] = "MorphStream is a high performance transactional SPE";
        testStrings[2] = "This project is designed for fault tolerance in MorphStream";
        Compressor[] compressors = new Compressor[3];
        compressors[0] = new NativeCompressor();
        compressors[1] = new RLECompressor();
        compressors[2] = new XORCompressor();
        for (int i = 0; i < compressors.length; i ++) {
            File file = new File("/Users/curryzjj/hair-loss/SC/MorphStreamDR/Benchmark/" + i + ".txt");
            BufferedWriter BufferedWriter = new BufferedWriter(new FileWriter(file, false));
            if (!file.exists()) {
                file.mkdirs();
            }
            for(String s : testStrings) {
                BufferedWriter.write(compressors[i].compress(s) + "\n");
            }
            BufferedWriter.flush();
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            String s = reader.readLine();
            while (s != null) {
                System.out.println(compressors[i].uncompress(s));
                s = reader.readLine();
            }
        }
    }
}
