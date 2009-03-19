/*
 * Created on Aug 22, 2005 by rob
 */
package ibis.impl.mpi;

import ibis.io.SerializationFactory;
import ibis.io.SerializationInput;
import ibis.io.SerializationOutput;

import java.util.HashMap;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IbisMPIInterface {

    private static final Logger logger
            = LoggerFactory.getLogger("ibis.impl.mpi.IbisMPIInterface");

    private static final boolean DEBUG = false;

    static final boolean SINGLE_THREAD = false;

    static final int MAX_POLLS = 100;

    static final int NANO_SLEEP_TIME = 0; // don't sleep between polls

    // static final int NANO_SLEEP_TIME = 10 * 1000; // 10 us

    static final int TYPE_BOOLEAN = 1;
    
    static final int TYPE_BYTE = 2;

    static final int TYPE_CHAR = 3;

    static final int TYPE_SHORT = 4;

    static final int TYPE_INT = 5;

    static final int TYPE_FLOAT = 6;

    static final int TYPE_LONG = 7;

    static final int TYPE_DOUBLE = 8;

    static final int TYPE_COUNT = 9;

    private synchronized native int init();

    private synchronized native int size();

    private synchronized native int rank();

    private synchronized native void end();

    synchronized native int send(Object buf, int offset, int count,
        int type, int dest, int tag);

    synchronized native int isend(Object buf, int offset, int count,
        int type, int dest, int tag);

    synchronized native int recv(Object buf, int offset, int count,
        int type, int src, int tag);

    synchronized native int irecv(Object buf, int offset, int count,
        int type, int src, int tag);

    native int testAny();

    native int getResultSize(Object buf, int offset, int count, int type);

    private static IbisMPIInterface instance;

    private int size;

    private int rank;

    private CommInfo poller = null;

    private HashMap<Integer, CommInfo> locks = new HashMap<Integer, CommInfo>();
    
    static synchronized IbisMPIInterface createMpi(Properties props) {
        if (instance == null) {
            String libPath = props.getProperty("ibis.mpi.libpath");
            String sep = System.getProperty("file.separator");

            if (libPath != null) {
                String s = System.mapLibraryName("IbisMPIInterface");
                System.load(libPath + sep + s);
            }
            instance = new IbisMPIInterface();
        }
        return instance;
    }

    static synchronized IbisMPIInterface getMpi() {
        return instance;
    }

    private IbisMPIInterface() {
        init();
        size = size();
        rank = rank();
    }

    int getSize() {
        return size;
    }

    int getRank() {
        return rank;
    }

    void doEnd() {
        end();
    }

    private void nanosleep(int time) {
        if (time <= 0) {
            Thread.yield();
            return;
        }
        try {
            Thread.sleep(0, time);
        } catch (Exception e) { // ignore 
        }
    }

    private synchronized boolean getPollToken(CommInfo myLock) {
        if (poller == null || myLock == poller) {
            poller = myLock;
            return true;
        }
        return false;
    }

    private synchronized void releasePoller(CommInfo id) {
        locks.remove(new Integer(id.getId()));
        // If id was the poller, select another poller.
        if (poller == id) {
            poller = null;
            for (CommInfo d : locks.values()) {
                poller = d;
                d.signal();
                return;
            }
        }
    }
    
    int doRecv(Object buf, int offset, int count, int type, int src,
            int tag) {
            if (DEBUG && logger.isTraceEnabled()) {
                logger.trace(rank + " doing recv, buflen = " + getBufLen(buf, type)
                        + " off = " + offset + " count = " + count + " type = "
                        + type + " src = " + src + " tag = " + tag);
            }

            if (SINGLE_THREAD) {
                return recv(buf, offset, count, type, src, tag);
            }

            // use asynchronous recv
            CommInfo myLock;
            synchronized(this) {
                int id = irecv(buf, offset, count, type, src, tag);
                myLock = new CommInfo(id, buf, offset, count, type);
                locks.put(new Integer(id), myLock);
            }

            int retval = waitOrPoll(myLock, buf, offset, count, type);

            if (DEBUG && logger.isTraceEnabled()) {
                logger.trace(rank + " recv done, buflen = " + getBufLen(buf, type)
                        + " off = " + offset + " count = " + count + " type = "
                        + type + " src = " + src + " tag = " + tag);
            }
            return retval;
        }


    int doSend(Object buf, int offset, int count, int type, int dest,
        int tag) {
        if (DEBUG && logger.isTraceEnabled()) {
            logger.trace(rank + " doing send, buflen = " + getBufLen(buf, type)
                + " off = " + offset + " count = " + count + " type = "
                + type + " dest = " + dest + " tag = " + tag);
        }

        if (SINGLE_THREAD) {
            return send(buf, offset, count, type, dest, tag);
        }

        // use asynchronous sends
        CommInfo myLock;
        synchronized(this) {
            int id = isend(buf, offset, count, type, dest, tag);
            myLock = new CommInfo(id, buf, offset, count, type);
            locks.put(new Integer(id), myLock);
        }
        int retval = waitOrPoll(myLock, buf, offset, count, type);
        if (DEBUG && logger.isTraceEnabled()) {
            logger.trace(rank + " send done, buflen = " + getBufLen(buf, type)
                + " off = " + offset + " count = " + count + " type = "
                + type + " dest = " + dest + " tag = " + tag);
        }
        return retval;
    }

    /**
     * Returns the number of bytes written/read.
     */
    private int waitOrPoll(CommInfo myLock, Object buf, int offset, int count, int type) {

        int id = myLock.getId();
        while (true) {
            boolean iAmPoller = getPollToken(myLock);
            if (iAmPoller) {
                // myLock may actually have been signalled now, so test
                // for that.
                if (myLock.hasReturnValue()) {
                    releasePoller(myLock);
                    return myLock.getReturnValue();
                }
                int polls = MAX_POLLS;
                while (--polls >= 0) {
                    synchronized(this) {
                        int resultId = testAny();
                        if (resultId != -1) {
                            Integer dd = new Integer(resultId);
                            CommInfo d = locks.get(dd);
                            int size = getResultSize(d.buf, d.offset, d.count, d.type);
                            if (resultId == id) {
                                releasePoller(myLock);
                                return size;
                            }
                            // some operation posted by another thread (not the poller)
                            // was done. Notify it.
                            d.setReturnValue(size);
                        } else {
                            // nothing finished
                        }
                    }
                }
                nanosleep(NANO_SLEEP_TIME);
            } else {
                if (myLock.waitForSignal()) {
                    releasePoller(myLock);
                    return myLock.getReturnValue();
                }
            }
        } // end of while
    }

    private int getBufLen(Object buf, int type) {
        switch (type) {
        case TYPE_BOOLEAN:
            return ((boolean[]) buf).length;
        case TYPE_BYTE:
            return ((byte[]) buf).length;
        case TYPE_CHAR:
            return ((char[]) buf).length;
        case TYPE_SHORT:
            return ((short[]) buf).length;
        case TYPE_INT:
            return ((int[]) buf).length;
        case TYPE_FLOAT:
            return ((float[]) buf).length;
        case TYPE_LONG:
            return ((long[]) buf).length;
        case TYPE_DOUBLE:
            return ((double[]) buf).length;
        default:
            System.err.println("UNKNOWN TYPE: " + type);
            return -1;
        }
    }

    /**
     * Test main entry.
     * @param args program arguments.
     */
    public static void main(String[] args) {

        boolean verify = true;

        IbisMPIInterface m = IbisMPIInterface.createMpi(System.getProperties());

        System.err.println("java = " + System.getProperty("java.vendor") + " "
            + System.getProperty("java.version"));

        System.err.println("rank = " + m.rank);
        System.err.println("size = " + m.size);

        try {
            if (m.rank == 0) {
                MpiDataOutputStream mout = new MpiDataOutputStream(1, 666);
                SerializationOutput out = SerializationFactory
                    .createSerializationOutput("ibis", mout);
                out.writeObject("Hello, world");
                out.close();
            } else {
                MpiDataInputStream min = new MpiDataInputStream(0, 666);
                SerializationInput in = SerializationFactory
                    .createSerializationInput("ibis", min);
                Object o = in.readObject();
                in.close();
                System.err.println("msg: " + o);
            }
        } catch (Exception e) {
            System.err.println("Error: " + e);
        }

        byte[] msg;
        int times;
        long start;
        long time;

        msg = new byte[1];

        times = 100000;
        start = System.currentTimeMillis();
        for (int i = 0; i < times; i++) {
            if (m.rank == 0) {
                m.doSend(msg, 0, msg.length, TYPE_BYTE, 1, 666);
                m.doRecv(msg, 0, msg.length, TYPE_BYTE, 1, 666);
            } else {
                m.doRecv(msg, 0, msg.length, TYPE_BYTE, 0, 666);
                m.doSend(msg, 0, msg.length, TYPE_BYTE, 0, 666);
            }
        }

        time = System.currentTimeMillis() - start;
        double usecs = (time * 1000.0) / times;

        System.err.println(m.rank + ": latency = " + usecs + " usecs/message");

        times = 1000000;
        for (int y = 1024; y <= 1024 * 1024; y *= 2) {
            msg = new byte[y];
            for (int i = 0; i < msg.length; i++) {
                msg[i] = (byte) i;
            }

            start = System.currentTimeMillis();
            for (int i = 0; i < times; i++) {
                if (m.rank == 0) {
                    m.doSend(msg, 0, msg.length, TYPE_BYTE, 1, 666);
                } else {
                    m.doRecv(msg, 0, msg.length, TYPE_BYTE, 0, 666);
                }

                if (verify) {
                    for (int x = 0; x < msg.length; x++) {
                        if (msg[x] != (byte) x) {
                            System.err.println("verify error");
                        }
                    }
                }
            }

            time = System.currentTimeMillis() - start;
            double secs = time / 1000.0;

            double mbytes = (msg.length * times) / (1024.0 * 1024.0);
            double thrp = mbytes / secs;
            System.err.println(m.rank + ": throughput = " + thrp
                + " mbytes/s for msg size " + y);

            times /= 2;
        }

        m.doEnd();
    }
}
