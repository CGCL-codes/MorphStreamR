package common.io.Encoding.decoder;


import common.io.Enums.DataType;
import common.io.Enums.Encoding;
import common.io.Exception.encoding.DecodingException;
import common.io.Utils.Binary;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

public abstract class Decoder {
    private static final String ERROR_MSG = "Decoder not found: %s , DataType is : %s";
    private Encoding type;
    public Decoder(Encoding type) {
        this.type = type;
    }
    public void setType(Encoding type) {
        this.type = type;
    }
    public Encoding getType() {
        return type;
    }
    public static Decoder getDecoderByType(Encoding encoding, DataType dataType) {
        return null;
    }
    public int readInt(ByteBuffer buffer) {
        throw new DecodingException("Method readInt is not supported by Decoder");
    }

    public boolean readBoolean(ByteBuffer buffer) {
        throw new DecodingException("Method readBoolean is not supported by Decoder");
    }

    public short readShort(ByteBuffer buffer) {
        throw new DecodingException("Method readShort is not supported by Decoder");
    }

    public long readLong(ByteBuffer buffer) {
        throw new DecodingException("Method readLong is not supported by Decoder");
    }

    public float readFloat(ByteBuffer buffer) {
        throw new DecodingException("Method readFloat is not supported by Decoder");
    }

    public double readDouble(ByteBuffer buffer) {
        throw new DecodingException("Method readDouble is not supported by Decoder");
    }

    public Binary readBinary(ByteBuffer buffer) {
        throw new DecodingException("Method readBinary is not supported by Decoder");
    }

    public BigDecimal readBigDecimal(ByteBuffer buffer) {
        throw new DecodingException("Method readBigDecimal is not supported by Decoder");
    }

    public abstract boolean hasNext(ByteBuffer buffer) throws IOException;

    public abstract void reset();
}
