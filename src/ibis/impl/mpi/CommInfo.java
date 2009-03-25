package ibis.impl.mpi;

import java.util.ArrayList;

/**
 * Container for return value of communication operation (isend or irecv).
 * Also serves as lock.
 */
public final class CommInfo {

    private static final ArrayList<CommInfo> infos = new ArrayList<CommInfo>();

    // Single linked list.
    private static CommInfo frees = null;

    // Double linked list.
    private static CommInfo busies = null;
    
    public static CommInfo getCommInfo(Object buf) {
        CommInfo result;
        if (frees == null) {
            result = new CommInfo(infos.size());
            infos.add(result);
        } else {
            result = frees;
            frees = frees.next;
            result.hasReturnValue = false;
            result.signalled = false;
        }
        if (busies != null) {
            busies.prev = result;
        }
        result.next = busies;
        result.prev = null;
        busies = result;
        result.buf = buf;
        return result;
    }
    
    public static void releaseCommInfo(CommInfo e) {
        if (e == busies) {
            busies = busies.next;
            if (busies != null) {
                busies.prev = null;
            }
        } else {
            e.prev.next = e.next;
            if (e.next != null) {
                e.next.prev = e.prev;
            }
        }
        e.next = frees;
        frees = e;
    }
    
    public static CommInfo getCommInfo(int id) {
        return infos.get(id);
    }
    
    public static CommInfo getBusy() {
        return busies;
    }
    
    /** Id of the communication operation. */
    private final int id;
    
    /** Return value of the communication operation. */
    private int retval;

    /** Link for frees or busies. */
    private CommInfo next;
    private CommInfo prev;

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
