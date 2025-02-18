package common.io.ByteIO.InputWithDecompression;

import common.io.ByteIO.DataInputView;

import java.io.IOException;
import java.nio.ByteBuffer;

public class NativeDataInputView extends DataInputView {
    public NativeDataInputView(ByteBuffer buffer) {
        super(buffer);
    }

    @Override
    public byte[] decompression(byte[] in, int length) {
        return in;
    }

    @Override
    public byte[] readFullyDecompression() throws IOException {
        byte[] b = new byte[readInt()];
        readFully(b);
        return b;
    }
}
