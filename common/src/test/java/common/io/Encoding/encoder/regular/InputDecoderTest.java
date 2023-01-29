package common.io.Encoding.encoder.regular;

import common.io.ByteIO.SyncFileAppender;
import common.io.Encoding.decoder.IntRleDecoder;
import common.io.Encoding.decoder.LongRleDecoder;
import common.io.Encoding.encoder.IntRleEncoder;
import common.io.Encoding.encoder.LongRleEncoder;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.nio.file.StandardOpenOption.READ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InputDecoderTest {
    public class InputTest{
        public int id;
        public int sourceAccountId;
        public int destinationAccountId;
        public long accountTransfer;
        public InputTest(int id, int sourceAccountId, int destinationAccountId, long accountTransfer) {
            this.id = id;
            this.sourceAccountId = sourceAccountId;
            this.destinationAccountId = destinationAccountId;
            this.accountTransfer = accountTransfer;
        }
    }
    public IntRleEncoder encoder = new IntRleEncoder();
    public LongRleEncoder longRleEncoder = new LongRleEncoder();
    public ByteArrayOutputStream baos = new ByteArrayOutputStream();
    public IntRleDecoder decoder1 = new IntRleDecoder();
    public IntRleDecoder decoder2 = new IntRleDecoder();
    public IntRleDecoder decoder3 = new IntRleDecoder();
    public LongRleDecoder decoder4 = new LongRleDecoder();
    public int position = 0;
    public List<InputTest> inputTests = new ArrayList<>();
    @Test
    public void testAll() throws Exception {
        testInput();
        testInput();
        readTest();
    }
    @Test
    public void testInput() throws IOException {
        InputTest[] input = new InputTest[4];
        for(int i = 0; i < input.length; i++) {
            input[i] = new InputTest(i, i + 1, i + 2, i + 3);
        }
        for (InputTest inputTest : input) {
            encoder.encode(inputTest.id, baos);
        }
        encoder.flush(baos);
        ByteBuffer byteBuffer1 = ByteBuffer.wrap(baos.toByteArray());
        int position1 = baos.size();
        baos.reset();

        for (InputTest inputTest : input) {
            encoder.encode(inputTest.sourceAccountId, baos);
        }
        encoder.flush(baos);
        ByteBuffer byteBuffer2 = ByteBuffer.wrap(baos.toByteArray());
        int position2 = baos.size() + position1;
        baos.reset();

        for (InputTest inputTest : input) {
            encoder.encode(inputTest.destinationAccountId, baos);
        }
        encoder.flush(baos);
        ByteBuffer byteBuffer3 = ByteBuffer.wrap(baos.toByteArray());
        int position3 = baos.size() + position2;
        baos.reset();

        for (InputTest inputTest : input) {
            longRleEncoder.encode(inputTest.accountTransfer, baos);
        }
        longRleEncoder.flush(baos);
        ByteBuffer byteBuffer4 = ByteBuffer.wrap(baos.toByteArray());
        int position4 = baos.size() + position3;
        baos.reset();
        Path path = Paths.get("/Users/curryzjj/hair-loss/SC/MorphStreamDR/Benchmark/" + InputDecoderTest.class.getName() + ".txt");
        try {
            SyncFileAppender appender = new SyncFileAppender(true, path);
            ByteBuffer metaBuffer = ByteBuffer.allocate(4 * 5);
            metaBuffer.putInt(0);
            metaBuffer.putInt((int) (position1 + appender.getPosition().get()));
            metaBuffer.putInt((int) (position2 + appender.getPosition().get()));
            metaBuffer.putInt((int) (position3 + appender.getPosition().get()));
            metaBuffer.putInt((int) (position4 + appender.getPosition().get()));
            metaBuffer.flip();
            appender.append(metaBuffer);
            appender.append(byteBuffer1);
            appender.append(byteBuffer2);
            appender.append(byteBuffer3);
            appender.append(byteBuffer4);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    @Test
    public void readTest() {
        Path path = Paths.get("/Users/curryzjj/hair-loss/SC/MorphStreamDR/Benchmark/" + InputDecoderTest.class.getName() + ".txt");
        AsynchronousFileChannel afc1 = null;
        try {
            afc1 = AsynchronousFileChannel.open(path, READ);
            int fileSize = (int) afc1.size();
            ByteBuffer dataBuffer = ByteBuffer.allocate(fileSize);
            Future<Integer> result1 = afc1.read(dataBuffer, 0);
            result1.get();
            dataBuffer.flip();
            while (dataBuffer.hasRemaining() && dataBuffer.getInt() == 0) {
                int position1 = dataBuffer.getInt();
                int position2 = dataBuffer.getInt();
                int position3 = dataBuffer.getInt();
                int position4 = dataBuffer.getInt();

                dataBuffer.limit( position1 + 20);
                ByteBuffer buffer1 = dataBuffer.slice();

                dataBuffer.position(position1 + 20);
                dataBuffer.limit( position2 + 20);
                ByteBuffer buffer2 = dataBuffer.slice();

                dataBuffer.position(position2 + 20);
                dataBuffer.limit(position3 + 20);
                ByteBuffer buffer3 = dataBuffer.slice();

                dataBuffer.position(position3 + 20);
                dataBuffer.limit(position4 + 20);
                ByteBuffer buffer4 = dataBuffer.slice();
                while (decoder1.hasNext(buffer1)) {
                    this.inputTests.add(new InputTest(decoder1.readInt(buffer1), decoder2.readInt(buffer2), decoder3.readInt(buffer3),decoder4.readLong(buffer4)));
                }
                decoder1.reset();
                decoder2.reset();
                decoder3.reset();
                decoder4.reset();
                dataBuffer.position(position4 + 20);
                dataBuffer.limit(dataBuffer.capacity());
            }
            File file = new File(path.toString());
            if (file.exists()) {
                file.delete();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
