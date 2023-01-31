package common.io.Compressor;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RLECompressorTest {
    public String testString = "MorphStreamDR is extend from MorphStream with FaultTolerance";
    RLECompressor rleCompressor = new RLECompressor();
    @Test
    public void testRLECompressor() throws Exception {
        String compressedString = rleCompressor.compress(testString);
        assertEquals(testString, rleCompressor.uncompress(compressedString));
    }
}
