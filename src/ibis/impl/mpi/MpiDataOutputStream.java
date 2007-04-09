/*
 * Created on Aug 23, 2005 by rob
 */
package ibis.impl.mpi;

import ibis.util.io.DataOutputStream;

import java.io.IOException;

public class MpiDataOutputStream extends DataOutputStream {
    int tag;
    int destRank;
    IbisMPIInterface mpi;
    int bytesWritten = 0;
    
    boolean[] tmpBoolean = new boolean[1];
    byte[] tmpByte = new byte[1];
    char[] tmpChar = new char[1];
    short[] tmpShort = new short[1];
    int[] tmpInt = new int[1];
    float[] tmpFloat = new float[1];
    long[] tmpLong = new long[1];
    double[] tmpDouble = new double[1];

    MpiDataOutputStream(int destRank, int tag) {
        this.mpi = IbisMPIInterface.getMpi();
        this.destRank = destRank;
        this.tag = tag;
    }

    public long bytesWritten() {
        return bytesWritten;
    }

    public void close() throws IOException {
        // nothing to do
    }

    public void flush() throws IOException {
        // nothing to do
    }

    public void resetBytesWritten() {
        bytesWritten = 0;
    }

    public void writeArray(boolean[] source, int offset, int length) throws IOException {
        mpi.doSend(source, offset, length, IbisMPIInterface.TYPE_BOOLEAN, destRank, tag);
    }

    public void write(byte[] source, int offset, int length) throws IOException {
        mpi.doSend(source, offset, length, IbisMPIInterface.TYPE_BYTE, destRank, tag);
    }

    public void writeArray(byte[] source, int offset, int length) throws IOException {
        mpi.doSend(source, offset, length, IbisMPIInterface.TYPE_BYTE, destRank, tag);
    }

    public void writeArray(char[] source, int offset, int length) throws IOException {
        mpi.doSend(source, offset, length, IbisMPIInterface.TYPE_CHAR, destRank, tag);
    }

    public void writeArray(double[] source, int offset, int length) throws IOException {
        mpi.doSend(source, offset, length, IbisMPIInterface.TYPE_DOUBLE, destRank, tag);
    }

    public void writeArray(float[] source, int offset, int length) throws IOException {
        mpi.doSend(source, offset, length, IbisMPIInterface.TYPE_FLOAT, destRank, tag);
    }

    public void writeArray(int[] source, int offset, int length) throws IOException {
        mpi.doSend(source, offset, length, IbisMPIInterface.TYPE_INT, destRank, tag);
    }

    public void writeArray(long[] source, int offset, int length) throws IOException {
        mpi.doSend(source, offset, length, IbisMPIInterface.TYPE_LONG, destRank, tag);
    }

    public void writeArray(short[] source, int offset, int length) throws IOException {
        mpi.doSend(source, offset, length, IbisMPIInterface.TYPE_SHORT, destRank, tag);
    }

    public void writeBoolean(boolean value) throws IOException {
        tmpBoolean[0] = value;
        mpi.doSend(tmpBoolean, 0, 1, IbisMPIInterface.TYPE_BOOLEAN, destRank, tag);
    }

    public void writeByte(byte value) throws IOException {
        tmpByte[0] = value;
        mpi.doSend(tmpByte, 0, 1, IbisMPIInterface.TYPE_BYTE, destRank, tag);
    }

    public void writeChar(char value) throws IOException {
        tmpChar[0] = value;
        mpi.doSend(tmpChar, 0, 1, IbisMPIInterface.TYPE_CHAR, destRank, tag);
    }

    public void writeDouble(double value) throws IOException {
        tmpDouble[0] = value;
        mpi.doSend(tmpDouble, 0, 1, IbisMPIInterface.TYPE_DOUBLE, destRank, tag);
    }

    public void writeFloat(float value) throws IOException {
        tmpFloat[0] = value;
        mpi.doSend(tmpFloat, 0, 1, IbisMPIInterface.TYPE_FLOAT, destRank, tag);
    }

    public void writeInt(int value) throws IOException {
        tmpInt[0] = value;
        mpi.doSend(tmpInt, 0, 1, IbisMPIInterface.TYPE_INT, destRank, tag);
    }

    public void writeLong(long value) throws IOException {
        tmpLong[0] = value;
        mpi.doSend(tmpLong, 0, 1, IbisMPIInterface.TYPE_LONG, destRank, tag);
    }

    public void writeShort(short value) throws IOException {
        tmpShort[0] = value;
        mpi.doSend(tmpShort, 0, 1, IbisMPIInterface.TYPE_SHORT, destRank, tag);
    }

    public void write(int value) throws IOException {
        tmpByte[0] = (byte)value;
        mpi.doSend(tmpByte, 0, 1, IbisMPIInterface.TYPE_BYTE, destRank, tag);
    }

    @Override
    public int bufferSize() {
        return -1;
    }
}
