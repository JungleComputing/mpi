/*
 * Created on Aug 22, 2005 by rob
 */
package ibis.impl.mpi;

import ibis.io.SerializationFactory;
import ibis.io.SerializationInput;
import ibis.io.SerializationOutput;
import ibis.util.TypedProperties;

import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IbisMPIInterface {

    private static final Logger logger
            = LoggerFactory.getLogger("ibis.impl.mpi.IbisMPIInterface");

    private static final boolean DEBUG = false;

    static final int TYPE_BOOLEAN = 1;
    
    static final int TYPE_BYTE = 2;

    static final int TYPE_CHAR = 3;

    static final int TYPE_SHORT = 4;

    static final int TYPE_INT = 5;

    static final int TYPE_FLOAT = 6;

    static final int TYPE_LONG = 7;

    static final int TYPE_DOUBLE = 8;

    static final int TYPE_COUNT = 9;

    private synchronized native int init(boolean threadSafeMPI);

    private synchronized native int size();

    private synchronized native int rank();

    private synchronized native void end();

    synchronized native int send(Object buf, int offset, int count,
        int type, int dest, int tag);

    synchronized native int isend(Object buf, int offset, int count,
        int type, int dest, int tag, int id);

    synchronized native int recv(Object buf, int offset, int count,
        int type, int src, int tag);

    synchronized native int irecv(Object buf, int offset, int count,
        int type, int src, int tag, int id);

    native int testAny();

    native int getResultSize(Object buf);

    private static IbisMPIInterface instance;

    private int size;

    private int rank;

    private final boolean threadSafeMPI;
    
    private final int maxPolls;

    private final int nanoSleepTime; // don't sleep between polls

    private CommInfo poller = null;
   
    static synchronized IbisMPIInterface createMpi(Properties props) {
        if (instance == null) {
            TypedProperties properties = new TypedProperties(props);
            String libPath = properties.getProperty("ibis.mpi.libpath");
            String sep = System.getProperty("file.separator");

            if (libPath != null) {
                String s = System.mapLibraryName("IbisMPIInterface");
                System.load(libPath + sep + s);
            }
            int polls = properties.getIntProperty("ibis.mpi.polls", 10);
            int nanoSleepTime = properties.getIntProperty("ibis.mpi.nanosleep", 0);

            boolean threadSafeMPI = properties.getBooleanProperty("ibis.mpi.threadsafe", true);
            
            instance = new IbisMPIInterface(polls, nanoSleepTime, threadSafeMPI);
        }
        return instance;
    }

    static synchronized IbisMPIInterface getMpi() {
        return instance;
    }

    private IbisMPIInterface(int polls, int nanoSleepTime, boolean threadSafeMPI) {
        this.maxPolls = polls;
        this.nanoSleepTime = nanoSleepTime;
        this.threadSafeMPI = threadSafeMPI;
        init(threadSafeMPI);
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
    
    int doRecv(Object buf, int offset, int count, int type, int src,
            int tag) throws IOException {
            if (DEBUG && logger.isTraceEnabled()) {
                logger.trace(rank + " doing recv, buflen = " + getBufLen(buf, type)
                        + " off = " + offset + " count = " + count + " type = "
                        + type + " src = " + src + " tag = " + tag);
            }

            if (threadSafeMPI) {
                return recv(buf, offset, count, type, src, tag);
            }

            // use asynchronous recv
            CommInfo myLock;
            
            synchronized(this) {
                myLock = CommInfo.getCommInfo(buf);
                if (irecv(buf, offset, count, type, src, tag, myLock.getId()) < 0) {
                    throw new IOException("irecv failed");
                }
                if (poller == null) {
                    poller = myLock;
                    poller.signal();
                }
            }

            int retval = waitOrPoll(myLock);

            if (DEBUG && logger.isTraceEnabled()) {
                logger.trace(rank + " recv done, buflen = " + getBufLen(buf, type)
                        + " off = " + offset + " count = " + count + " type = "
                        + type + " src = " + src + " tag = " + tag);
            }
            return retval;
        }


    void doSend(Object buf, int offset, int count, int type, int dest,
        int tag) throws IOException {
        if (DEBUG && logger.isTraceEnabled()) {
            logger.trace(rank + " doing send, buflen = " + getBufLen(buf, type)
                + " off = " + offset + " count = " + count + " type = "
                + type + " dest = " + dest + " tag = " + tag);
        }

        if (threadSafeMPI) {
            send(buf, offset, count, type, dest, tag);
            return;
        }

        // use asynchronous sends
        CommInfo myLock;

        synchronized(this) {
            myLock = CommInfo.getCommInfo(buf);
            int retval = isend(buf, offset, count, type, dest, tag, myLock.getId());
            if (retval < 0) {
                throw new IOException("isend failed");
            }
            if (retval > 0) {
                // send already finished!
                if (DEBUG && logger.isTraceEnabled()) {
                    logger.trace(rank + " quick send done, buflen = "
                            + getBufLen(buf, type) + " off = " + offset
                            + " count = " + count + " type = " + type +
                            " dest = " + dest + " tag = " + tag);
                }
                CommInfo.releaseCommInfo(myLock);
                return;
            }
            if (poller == null) {
                poller = myLock;
                poller.signal();
            }
        }
        
        waitOrPoll(myLock);
        
        if (DEBUG && logger.isTraceEnabled()) {
            logger.trace(rank + " send done, buflen = " + getBufLen(buf, type)
                + " off = " + offset + " count = " + count + " type = "
                + type + " dest = " + dest + " tag = " + tag);
        }
    }

    private void poll(int id) {
        for (int polls = maxPolls; polls > 0; polls--) {
            synchronized(this) {
                int resultId = testAny();
                if (resultId != -1) {
                    CommInfo d = CommInfo.getCommInfo(resultId);
                    int size = getResultSize(d.buf);
                    d.setReturnValue(size);
                    if (resultId == id) {
                        return;
                    }
                }
            }
        }
        nanosleep(nanoSleepTime);
    }

    /**
     * Returns the number of bytes written/read.
     */
    private int waitOrPoll(CommInfo myLock) {
	int id = myLock.getId();
        while (true) {
            if (myLock.waitForSignal()) {
                // I have a return value!
                int retval = myLock.getReturnValue();
                synchronized(this) {
                    CommInfo.releaseCommInfo(myLock);
                    // If myLock was the poller, select another poller.
                    if (poller == myLock) {
                        poller = CommInfo.getBusy();
                        if (poller != null) {
                            poller.signal();
                        }
                    }
                }
                return retval;
            }
            // Other reason I am being signalled: I became poller.
            poll(id);
        }
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
    public static void main(String[] args) throws Exception {

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
