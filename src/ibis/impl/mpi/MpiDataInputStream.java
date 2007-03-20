/*
 * Created on Aug 23, 2005 by rob
 */
package ibis.impl.mpi;

import ibis.io.DataInputStream;

import java.io.IOException;

// Note: this stream implementation does NOT support SUN serialization!

public class MpiDataInputStream extends DataInputStream {
    boolean[] tmpBoolean = new boolean[1];
    byte[] tmpByte = new byte[1];
    char[] tmpChar = new char[1];
    short[] tmpShort = new short[1];
    int[] tmpInt = new int[1];
    float[] tmpFloat = new float[1];
    long[] tmpLong = new long[1];
    double[] tmpDouble = new double[1];
    
    int srcRank;

    int srcTag;

    int bytesRead = 0;

    IbisMPIInterface mpi;

    MpiDataInputStream(int srcRank, int srcTag) {
        this.srcRank = srcRank;
        this.srcTag = srcTag;
        this.mpi = IbisMPIInterface.getMpi();
    }

    public long bytesRead() {
        return bytesRead;
    }

    public void close() throws IOException {
        // nothing to do
    }

    public boolean readBoolean() throws IOException {
        mpi.doRecv(tmpBoolean, 0, 1, IbisMPIInterface.TYPE_BOOLEAN, srcRank,
                srcTag);
        return tmpBoolean[0];
    }

    public void resetBytesRead() {
        bytesRead = 0;
    }

    public void readArray(boolean[] destination, int offset, int length) 
        throws IOException {
    mpi.doRecv(destination, offset, length, IbisMPIInterface.TYPE_BOOLEAN, srcRank,
            srcTag);
    }

    public void readArray(byte[] destination, int offset, int length)
            throws IOException {
        mpi.doRecv(destination, offset, length, IbisMPIInterface.TYPE_BYTE, srcRank,
                srcTag);
    }

    public void readArray(char[] destination, int offset, int length)
            throws IOException {
        mpi.doRecv(destination, offset, length, IbisMPIInterface.TYPE_CHAR, srcRank,
                srcTag);
    }

    public void readArray(double[] destination, int offset, int length)
            throws IOException {
        mpi.doRecv(destination, offset, length, IbisMPIInterface.TYPE_DOUBLE, srcRank,
                srcTag);
    }

    public void readArray(float[] destination, int offset, int length)
            throws IOException {
        mpi.doRecv(destination, offset, length, IbisMPIInterface.TYPE_FLOAT, srcRank,
                srcTag);
    }

    public void readArray(int[] destination, int offset, int length)
            throws IOException {
        mpi.doRecv(destination, offset, length, IbisMPIInterface.TYPE_INT, srcRank,
                srcTag);
        }

    public void readArray(long[] destination, int offset, int length)
            throws IOException {
        mpi.doRecv(destination, offset, length, IbisMPIInterface.TYPE_LONG, srcRank,
                srcTag);
    }

    public void readArray(short[] destination, int offset, int length)
            throws IOException {
        mpi.doRecv(destination, offset, length, IbisMPIInterface.TYPE_SHORT, srcRank,
                srcTag);
    }

    public byte readByte() throws IOException {
        mpi.doRecv(tmpByte, 0, 1, IbisMPIInterface.TYPE_BYTE, srcRank,
                srcTag);
        return tmpByte[0];
    }

    public char readChar() throws IOException {
        mpi.doRecv(tmpChar, 0, 1, IbisMPIInterface.TYPE_CHAR, srcRank,
                srcTag);
        return tmpChar[0];
    }

    public double readDouble() throws IOException {
        mpi.doRecv(tmpDouble, 0, 1, IbisMPIInterface.TYPE_DOUBLE, srcRank,
                srcTag);
        return tmpDouble[0];
    }

    public float readFloat() throws IOException {
        mpi.doRecv(tmpFloat, 0, 1, IbisMPIInterface.TYPE_FLOAT, srcRank,
                srcTag);
        return tmpFloat[0];
    }

    public int readInt() throws IOException {
        mpi.doRecv(tmpInt, 0, 1, IbisMPIInterface.TYPE_INT, srcRank,
                srcTag);
        return tmpInt[0];
    }

    public long readLong() throws IOException {
        mpi.doRecv(tmpLong, 0, 1, IbisMPIInterface.TYPE_LONG, srcRank,
                srcTag);
        return tmpLong[0];
    }

    public short readShort() throws IOException {
        mpi.doRecv(tmpShort, 0, 1, IbisMPIInterface.TYPE_SHORT, srcRank,
                srcTag);
        return tmpShort[0];
    }

    public int read() throws IOException {
        int sz = mpi.doRecv(tmpByte, 0, 1, IbisMPIInterface.TYPE_BYTE, srcRank,
                srcTag);
        if (sz == 0) {
            return -1;
        }
        return tmpByte[0] & 0377;
    }

    public int read(byte[] buf) throws IOException {
        return read(buf, 0, buf.length);
    }

    public int read(byte[] destination, int offset, int length)
            throws IOException {
        return mpi.doRecv(destination, offset, length,
                IbisMPIInterface.TYPE_BYTE, srcRank, srcTag);
    }
}
