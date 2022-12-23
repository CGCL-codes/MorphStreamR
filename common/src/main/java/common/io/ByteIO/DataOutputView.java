package common.io.ByteIO;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class DataOutputView {
    private ByteArrayOutputStream out = new ByteArrayOutputStream();
    private byte writeBuffer[] = new byte[8];
    public synchronized void write(byte b[]) throws IOException {
        out.write(b);
    }
    public synchronized void write(byte b[], int off, int len)
            throws IOException
    {
        out.write(b, off, len);
    }
    public final void writeBoolean(boolean v) throws IOException {
        out.write(v ? 1 : 0);
    }
    public final void writeShort(int v) throws IOException {
        out.write((v >>> 8) & 0xFF);
        out.write((v >>> 0) & 0xFF);
    }
    public final void writeChar(int v) throws IOException {
        out.write((v >>> 8) & 0xFF);
        out.write((v >>> 0) & 0xFF);
    }
    public final void writeInt(int v) throws IOException {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>>  8) & 0xFF);
        out.write((v >>>  0) & 0xFF);
    }
    public final void writeLong(long v) throws IOException {
        writeBuffer[0] = (byte)(v >>> 56);
        writeBuffer[1] = (byte)(v >>> 48);
        writeBuffer[2] = (byte)(v >>> 40);
        writeBuffer[3] = (byte)(v >>> 32);
        writeBuffer[4] = (byte)(v >>> 24);
        writeBuffer[5] = (byte)(v >>> 16);
        writeBuffer[6] = (byte)(v >>>  8);
        writeBuffer[7] = (byte)(v >>>  0);
        out.write(writeBuffer);
    }
    public final void writeFloat(float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }
    public final void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }
    public final void writeBytes(String s) throws IOException {
        int len = s.length();
        for (int i = 0 ; i < len ; i++) {
            out.write((byte)s.charAt(i));
        }
    }
    public final void writeChars(String s) throws IOException {
        int len = s.length();
        for (int i = 0 ; i < len ; i++) {
            int v = s.charAt(i);
            out.write((v >>> 8) & 0xFF);
            out.write((v >>> 0) & 0xFF);
        }
    }

    public byte[] getByteArray() {
        return this.out.toByteArray();
    }
}
