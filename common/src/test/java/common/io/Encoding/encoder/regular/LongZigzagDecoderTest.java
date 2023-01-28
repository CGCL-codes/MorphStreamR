package common.io.Encoding.encoder.regular;

import common.io.Encoding.decoder.Decoder;
import common.io.Encoding.decoder.LongZigzagDecoder;
import common.io.Encoding.encoder.Encoder;
import common.io.Encoding.encoder.LongZigzagEncoder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class LongZigzagDecoderTest {
    private static final Logger logger = LoggerFactory.getLogger(LongZigzagDecoderTest.class);
    private List<Long> longList;
    private Random rand = new Random();
    private List<Long> randomLongList;

    @Before
    public void setUp() {
        longList = new ArrayList<>();
        int int_num = 10000;
        randomLongList = new ArrayList<>();
        for (int i = 0; i < int_num; i++) {
            longList.add((long) (i + ((long) 1 << 31)));
        }
        for (int i = 0; i < int_num; i++) {
            randomLongList.add(rand.nextLong());
        }
    }

    @After
    public void tearDown() {
        randomLongList.clear();
        longList.clear();
    }

    @Test
    public void testZigzagReadLong() throws Exception {
        for (int i = 1; i < 10; i++) {
            testLong(longList, false, i);
            testLong(randomLongList, false, i);
        }
    }

    private void testLong(List<Long> list, boolean isDebug, int repeatCount) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Encoder encoder = new LongZigzagEncoder();
        for (int i = 0; i < repeatCount; i++) {
            for (long value : list) {
                encoder.encode(value, baos);
            }
            encoder.flush(baos);
        }

        ByteBuffer bais = ByteBuffer.wrap(baos.toByteArray());
        Decoder decoder = new LongZigzagDecoder();
        for (int i = 0; i < repeatCount; i++) {
            for (long value : list) {
                long value_ = decoder.readLong(bais);
                if (isDebug) {
                    logger.debug("{} // {}", value_, value);
                }
                assertEquals(value, value_);
            }
        }
    }
}
