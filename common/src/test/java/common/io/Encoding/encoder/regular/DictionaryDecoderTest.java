package common.io.Encoding.encoder.regular;

import common.io.ByteIO.SyncFileAppender;
import common.io.Encoding.decoder.DictionaryDecoder;
import common.io.Encoding.encoder.DictionaryEncoder;
import common.io.Utils.Binary;
import org.apache.log4j.Layout;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.nio.file.StandardOpenOption.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DictionaryDecoderTest {
    private DictionaryEncoder encoder = new DictionaryEncoder();
    private DictionaryDecoder decoder = new DictionaryDecoder();
    private ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private long position;

    @Test
    public void testSingle() {
        testAll("a");
        testAll("b");
        testAll("c");
    }

    @Test
    public void testAllUnique() {
        testAll("a", "b", "c");
        testAll("x", "o", "q");
        testAll(",", ".", "c", "b", "e");
    }

    @Test
    public void testAllSame() {
        testAll("a", "a", "a");
        testAll("b", "b", "b");
    }

    @Test
    public void testMixed() {
        // all characters
        String[] allChars = new String[256];
        allChars[0] = "" + (char) ('a' + 1);
        for (int i = 0; i < 256; i++) {
            allChars[i] = "" + (char) (i) + (char) (i) + (char) (i);
        }
        testAll(allChars);
    }
    @Test
    public void testNIO() {
        testIO("a", "b", "c");
    }
    private void testAll(String... all) {
        for (String s : all) {
            encoder.encode(new Binary(s), baos);
        }
        encoder.flush(baos);

        ByteBuffer out = ByteBuffer.wrap(baos.toByteArray());

        for (String s : all) {
            assertTrue(decoder.hasNext(out));
            assertEquals(s, decoder.readBinary(out).getStringValue());
        }

        decoder.reset();
        baos.reset();
    }
    private void testIO(String... all) {
        for (String s : all) {
            encoder.encode(new Binary(s), baos);
        }
        encoder.flush(baos);
        ByteBuffer out = ByteBuffer.wrap(baos.toByteArray());
        Path path = Paths.get("/Users/curryzjj/hair-loss/SC/MorphStreamDR/Benchmark/" + DictionaryDecoderTest.class.getName() + ".txt");
        try {
            SyncFileAppender appender = new SyncFileAppender(true, path);
            appender.append(out);
            AsynchronousFileChannel afc1 = AsynchronousFileChannel.open(path, READ);
            int fileSize = (int) afc1.size();
            ByteBuffer dataBuffer = ByteBuffer.allocate(fileSize);
            Future<Integer> result1 = afc1.read(dataBuffer, 0);
            result1.get();
            dataBuffer.flip();
            for (String s : all) {
                assertTrue(decoder.hasNext(dataBuffer));
                assertEquals(s, decoder.readBinary(dataBuffer).getStringValue());
            }
            decoder.reset();
            baos.reset();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
