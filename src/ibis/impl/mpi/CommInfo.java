package ibis.impl.mpi;

import java.util.ArrayList;

/**
 * Container for return value of communication operation (isend or irecv).
 * Also serves as lock.
 */
public final class CommInfo {
    
    private static final ArrayList<CommInfo> infos = new ArrayList<CommInfo>();
    
    private static final ArrayList<CommInfo> frees = new ArrayList<CommInfo>();
    
    private static final ArrayList<CommInfo> busies = new ArrayList<CommInfo>();
    
    public static CommInfo getCommInfo(Object buf) {
        int freeSize = frees.size();
        CommInfo result;
        if (freeSize == 0) {
            result = new CommInfo(infos.size());
            infos.add(result);
        } else {
            result = frees.remove(freeSize - 1);
            result.hasReturnValue = false;
            result.signalled = false;
        }
        busies.add(result);
        result.buf = buf;
        return result;
    }
    
    public static void releaseCommInfo(CommInfo e) {
        frees.add(e);
        busies.remove(e);
    }
    
    public static CommInfo getCommInfo(int id) {
        return infos.get(id);
    }
    
    public static CommInfo getBusy() {
        if (busies.size() == 0) {
            return null;
        }
        return busies.get(0);
    }
    
    /** Id of the communication operation. */
    private final int id;
    
    /** Return value of the communication operation. */
    private int retval;

    Object buf;

    private boolean signalled = false;
    
    private boolean hasReturnValue = false;
    
    private CommInfo(int id) {
        this.id = id;
    }
    
    public int getId() {
        return id;
    }
    
    public void setReturnValue(int retval) {
        hasReturnValue = true;
        this.retval = retval;
        signal();
    }
    
    public int getReturnValue() {
        return retval;
    }
    
    public synchronized boolean waitForSignal() {
        while (! signalled) {
            try {
                wait();
            } catch (InterruptedException e) {
                // ignored
            }
        }
        return hasReturnValue;
    }

    public boolean hasReturnValue() {
        return hasReturnValue;
    }
    
    public synchronized void signal() {
        signalled = true;
        notifyAll();
    }
    
    public int hashCode() {
        return id;
    }
}
