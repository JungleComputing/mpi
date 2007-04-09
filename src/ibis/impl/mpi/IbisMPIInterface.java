/*
 * Created on Aug 22, 2005 by rob
 */
package ibis.impl.mpi;

import ibis.ipl.IbisFactory;
import ibis.util.io.SerializationBase;
import ibis.util.io.SerializationInput;
import ibis.util.io.SerializationOutput;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

public class IbisMPIInterface {

    static final boolean SINGLE_THREAD = false;

    static final int MAX_POLLS = 100;

    static final boolean ONE_POLLER = true;

    static final boolean DEBUG = false;

    static final int SINGLE_POLLER_NANO_SLEEP_TIME = -1; // don't sleep between polls

    static final int NANO_SLEEP_TIME = -1; // don't sleep between polls

    // static final int NANO_SLEEP_TIME = 10 * 1000; // 10 us

    public static final int TYPE_BOOLEAN = 1;

    public static final int TYPE_BYTE = 2;

    public static final int TYPE_CHAR = 3;

    public static final int TYPE_SHORT = 4;

    public static final int TYPE_INT = 5;

    public static final int TYPE_FLOAT = 6;

    public static final int TYPE_LONG = 7;

    public static final int TYPE_DOUBLE = 8;

    public static final int TYPE_COUNT = 9;

    private synchronized native int init();

    private synchronized native int size();

    private synchronized native int rank();

    private synchronized native void end();

    public synchronized native int send(Object buf, int offset, int count,
        int type, int dest, int tag);

    public synchronized native int isend(Object buf, int offset, int count,
        int type, int dest, int tag);

    public synchronized native int recv(Object buf, int offset, int count,
        int type, int src, int tag);

    public synchronized native int irecv(Object buf, int offset, int count,
        int type, int src, int tag);

    public synchronized native int test(int id, Object buf, int offset,
        int count, int type);

    /** 
     * 
     * @return returns id of a send/recv that is done, or -1 if no operation is finished
     */
    public synchronized native int testAll();

    private static IbisMPIInterface instance;

    private int size;

    private int rank;

    private boolean pollToken = false;

    private HashMap<Integer, Integer> locks = new HashMap<Integer, Integer>();

    private HashSet<Integer> done = new HashSet<Integer>();

    static synchronized IbisMPIInterface createMpi(Properties props) {
        if (instance == null) {
            IbisFactory.loadLibrary("IbisMPIInterface", props);
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

    private synchronized boolean getPollToken() {
        if (!pollToken) {
            pollToken = true;
            return true;
        }

        return false;
    }

    private synchronized void releasePollToken() {
        pollToken = false;
    }

    public int doSend(Object buf, int offset, int count, int type, int dest,
        int tag) {
        if (DEBUG) {
            System.err
                .println(rank + " doing send, buflen = " + getBufLen(buf, type)
                    + " off = " + offset + " count = " + count + " type = "
                    + type + " dest = " + dest + " tag = " + tag);
            new Exception().printStackTrace();
        }

        if (SINGLE_THREAD) {
            return send(buf, offset, count, type, dest, tag);
        }

        // use asynchronous sends
        int id = isend(buf, offset, count, type, dest, tag);
        return waitOrPoll(id, buf, offset, count, type);
    }

    private int waitOrPoll(int id, Object buf, int offset, int count, int type) {

        if (!ONE_POLLER) {
            while (true) {
                int flag = test(id, buf, offset, count, type);
                if (flag != 0) {
                    return 1;
                }

                nanosleep(NANO_SLEEP_TIME);
            }
        }

        // there is only one poller thread

        Integer myLock = new Integer(id);
        while (true) {
            boolean iAmPoller = getPollToken();
            if (iAmPoller) {
                int polls = MAX_POLLS;
                while (polls >= 0) {
                    int finishedId = testAll();
                    if (finishedId == id) {
                        // my operation is done!
                        // we have to do a test to release buffers
                        int flag = test(id, buf, offset, count, type);
                        if (flag == 0) {
                            throw new Error(
                                "internal error, testAll returned my id, but test failed");
                        }
                        releasePollToken();

                        synchronized(this) {
                            for (Integer d : locks.keySet()) {
                                synchronized(d) {
                                    d.notifyAll();
                                }
                            }
                        }

                        return 1;
                    } else if (finishedId >= 0) {
                        // some operation posted by another thread (not the poller)
                        // was done. Notify it.
                        Integer d;
                        synchronized (this) {
                            Integer dd = new Integer(finishedId);
                            d = locks.get(dd);
                        }
                        if (d != null) {
                            synchronized(d) {
                                done.add(d);
                                d.notifyAll();
                            }
                        }
                    } else {
                        // nothing finished
                    }
                }
                nanosleep(SINGLE_POLLER_NANO_SLEEP_TIME);
            } else {
                // I am not the poller
                synchronized(this) {
                    locks.put(myLock, myLock);
                }
                synchronized(myLock) {
                    if (! done.contains(myLock)) {
                        try {
                            myLock.wait();
                        } catch (Exception e) {
                            // ignore
                        }
                    }
                    done.remove(myLock);
                }
                synchronized(this) {
                    locks.remove(myLock);
                }
                
                // poll once
                int flag = test(id, buf, offset, count, type);
                if (flag != 0) {
                    return 1;
                }
            }
        } // end of while
    }

    public int doRecv(Object buf, int offset, int count, int type, int src,
        int tag) {
        if (DEBUG) {
            System.err
                .println(rank + " doing recv, buflen = " + getBufLen(buf, type)
                    + " off = " + offset + " count = " + count + " type = "
                    + type + " src = " + src + " tag = " + tag);
            new Exception().printStackTrace();
        }

        if (SINGLE_THREAD) {
            return recv(buf, offset, count, type, src, tag);
        }

        // use asynchronous recv
        int id = irecv(buf, offset, count, type, src, tag);
        return waitOrPoll(id, buf, offset, count, type);
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
                SerializationOutput out = SerializationBase
                    .createSerializationOutput("ibis", mout);
                out.writeObject("Hello, world");
                out.close();
            } else {
                MpiDataInputStream min = new MpiDataInputStream(0, 666);
                SerializationInput in = SerializationBase
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
        double usecs = ((double) time * 1000.0) / times;

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
            double secs = (double) time / 1000.0;

            double mbytes = (msg.length * times) / (1024.0 * 1024.0);
            double thrp = mbytes / secs;
            System.err.println(m.rank + ": throughput = " + thrp
                + " mbytes/s for msg size " + y);

            times /= 2;
        }

        m.doEnd();
    }
}
