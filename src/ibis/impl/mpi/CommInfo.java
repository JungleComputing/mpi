package ibis.impl.mpi;

/**
 * Container for return value of communication operation (isend or irecv).
 * Also serves as lock.
 */
public final class CommInfo {
    /** Id of the communication operation. */
    private final int id;
    
    /** Return value of the communication operation. */
    private int retval;

    Object buf;

    int count;
    
    int type;

    int offset;
    
    private boolean signalled = false;
    
    private boolean hasReturnValue = false;
    
    public CommInfo(int id, Object buf, int offset, int count, int type) {
        this.id = id;
        this.buf = buf;
        this.count = count;
        this.offset = offset;
        this.type = type;
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
}
