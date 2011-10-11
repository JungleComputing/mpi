/* $Id: MpiReceivePort.java 5206 2007-03-14 08:53:14Z ceriel $ */

package ibis.impl.mpi;

import ibis.ipl.MessageUpcall;
import ibis.ipl.PortType;
import ibis.ipl.ReceivePortConnectUpcall;
import ibis.ipl.impl.Ibis;
import ibis.ipl.impl.ReadMessage;
import ibis.ipl.impl.ReceivePort;
import ibis.ipl.impl.ReceivePortConnectionInfo;
import ibis.ipl.impl.ReceivePortIdentifier;
import ibis.ipl.impl.SendPortIdentifier;
import ibis.util.ThreadPool;
import ibis.io.BufferedArrayInputStream;
import ibis.io.Conversion;
import ibis.io.DataInputStream;

import java.io.IOException;
import java.util.Properties;

class MpiReceivePort extends ReceivePort implements MpiProtocol {

    class ConnectionHandler extends ReceivePortConnectionInfo 
            implements Runnable, MpiProtocol {

        ConnectionHandler(SendPortIdentifier origin,
                ReceivePort port, DataInputStream in)
                throws IOException {
            super(origin, port, in);
        }

        public void run() {
            logger.info("Started connection handler thread");
            try {
                if (lazy_connectionhandler_thread) {
                    // For disconnects, there must be a reader thread, but we
                    // don't really want that. So, we have a thread that only
                    // checks every second.
                    for (;;) {
                	try {
                	    Thread.sleep(100);
                	} catch(Throwable e) {
                	    // ignore
                	}
                        synchronized(port) {
                            // If there is a reader, or a message is active,
                            // continue.
                            if (reader_busy || ((MpiReceivePort)port).getPortMessage() != null) {
                                continue;
                            }
                            if (in == null) {
                                return;
                            }
                            reader_busy = true;
                        }
                        if (logger.isInfoEnabled()) {
                            logger.info("Lazy thread starting read ...");
                        }
                        reader(true);
                        synchronized(port) {
                            reader_busy = false;
                            port.notifyAll();
                        }
                    }
                }
                else {
                    reader(true);
                }
            } catch (Throwable e) {
                logger.info("ConnectionHandler.run, connected "
                        + "to " + origin + ", caught exception", e);
                close(e);
            }
        }

        protected void upcallCalledFinish() {
            super.upcallCalledFinish();
            ThreadPool.createNew(this, "ConnectionHandler");
        }

        void reader(boolean fromHandlerThread) throws IOException {
            byte opcode = -1;

            if (in == null) {
                newStream();
            }
            while (in != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug(name + ": handler for " + origin + " woke up");
                }
                opcode = in.readByte();
                switch (opcode) {
                case NEW_RECEIVER:
                    if (logger.isDebugEnabled()) {
                        logger.debug(name + ": Got a NEW_RECEIVER from "
                                + origin);
                    }
                    newStream();
                    break;
                case NEW_MESSAGE:
                    if (logger.isDebugEnabled()) {
                        logger.debug(name + ": Got a NEW_MESSAGE from "
                                + origin);
                    }
                    message.setFinished(false);
                    if (numbered) {
                        message.setSequenceNumber(message.readLong());
                    }
                    ReadMessage m = message;
                    messageArrived(m, fromHandlerThread);
                    // Note: if upcall calls finish, a new message is
                    // allocated, so we cannot look at "message" anymore.
                    if (lazy_connectionhandler_thread || ! fromHandlerThread
                            || m.finishCalledInUpcall()) {
                        return;
                    }
                    break;
                case CLOSE_ALL_CONNECTIONS:
                    if (logger.isDebugEnabled()) {
                        logger.debug(name
                                + ": Got a CLOSE_ALL_CONNECTIONS from "
                                + origin);
                    }
                    close(null);
                    return;
                case CLOSE_ONE_CONNECTION:
                    if (logger.isDebugEnabled()) {
                        logger.debug(name + ": Got a CLOSE_ONE_CONNECTION from "
                                + origin);
                    }
                    // read the receiveport identifier from which the sendport
                    // disconnects.
                    byte[] length = new byte[Conversion.INT_SIZE];
                    in.readArray(length);
                    byte[] bytes = new byte[Conversion.defaultConversion
                            .byte2int(length, 0)];
                    in.readArray(bytes);
                    ReceivePortIdentifier identifier
                            = new ReceivePortIdentifier(bytes);
                    if (ident.equals(identifier)) {
                        // Sendport is disconnecting from me.
                        if (logger.isDebugEnabled()) {
                            logger.debug(name + ": disconnect from " + origin);
                        }
                        close(null);
                        synchronized(port) {
                            processedDisconnects++;
                            port.notifyAll();
                        }
                    }
                    break;
                default:
                    throw new IOException(name + ": Got illegal opcode "
                            + opcode + " from " + origin);
                }
            }
        }
    }

    private final boolean lazy_connectionhandler_thread;

    private boolean reader_busy = false;

    private int numDisconnects = 0;
  
    private int processedDisconnects = 0;

    MpiReceivePort(Ibis ibis, PortType type, String name, MessageUpcall upcall,
            ReceivePortConnectUpcall connUpcall, Properties props) throws IOException {
        super(ibis, type, name, upcall, connUpcall, props);

        // Why not CONNECTION_ONE_TO_MANY?
        // upcall requires handler thread,
        // connUpcall requires handler thread because if there is no read,
        // a lost connection would go by unnoticed.
        // For polling, a connection handler thread is required.
        // For MANY_TO_ONE/MANY_TO_MANY, each incoming connection requires a
        // separate thread. But ONE_TO_MANY? Is not different to ONE_TO_ONE from
        // the receiveport perspective, or is it???
        lazy_connectionhandler_thread = upcall == null && connUpcall == null
                && (type.hasCapability(PortType.CONNECTION_ONE_TO_ONE)
                    || type.hasCapability(PortType.CONNECTION_ONE_TO_MANY))
                && !type.hasCapability(PortType.RECEIVE_POLL)
                && !type.hasCapability(PortType.RECEIVE_TIMEOUT);
    }

    private ReadMessage getPortMessage() {
        return message;
    }

    synchronized void waitForDisconnects() {
        while (numDisconnects > processedDisconnects) {
            try {
                wait();
            } catch(Throwable e) {
                // ignored
            }
        }
    }
    
    synchronized void addDisconnect() {
        numDisconnects++;
    }
 
    private void messageArrived(ReadMessage msg, boolean fromHandlerThread) {
        super.messageArrived(msg);
        if (fromHandlerThread && upcall == null) {
            synchronized(this) {
                // Wait until the message is finished before starting to
                // read from the stream again ...
                while (! msg.isFinished()) {
                    try {
                        wait();
                    } catch(Exception e) {
                        // Ignored
                    }
                }
            }
        }
    }

    public ReadMessage getMessage(long timeout) throws IOException {
        if (lazy_connectionhandler_thread) {
            // Allow only one reader in.
            synchronized(this) {
                if (message != null && ! delivered) {
                    return super.getMessage(timeout);
                }
                while (reader_busy && ! closed) {
                    try {
                        wait();
                    } catch(Exception e) {
                        // ignored
                    }
                    if (message != null && ! delivered) {
                        return super.getMessage(timeout);
                    }
                }
                if (closed) {
                    throw new IOException("receive() on closed port");
                }
                reader_busy = true;
            }
            // Since we don't have any threads or timeout here, this 'reader' 
            // call directly handles the receive.              
            for (;;) {
                // Wait until there is a connection            
                synchronized(this) {
                    while (connections.size() == 0 && ! closed) {
                        try {
                            wait();
                        } catch (Exception e) {
                            /* ignore */
                        }
                    }

                    // Wait until the current message is done
                    while (message != null && ! closed) {
                        try {
                            wait();
                        } catch (Exception e) {
                            /* ignore */
                        }
                    }
                    if (closed) {
                        reader_busy = false;
                        notifyAll();
                        throw new IOException("receive() on closed port");
                    }
                }

                ReceivePortConnectionInfo conns[] = connections();
                // Note: This call does NOT always result in a message!
                ((ConnectionHandler)conns[0]).reader(false);
                synchronized(this) {
                    if (message != null) {
                        delivered = true;
                        reader_busy = false;
                        notifyAll();
                        return message;
                    }
                }
            }
        } else {
            return super.getMessage(timeout);
        }
    }

    synchronized void connect(SendPortIdentifier origin, int tag, int rank)
            throws IOException {
        DataInputStream in = new MpiDataInputStream(rank, tag);
        if (((MpiIbis) ibis).useBufferedArrayStreams) {
            in = new BufferedArrayInputStream(in, 4096);
        }
        ConnectionHandler conn = new ConnectionHandler(origin, this, in);
        ThreadPool.createNew(conn, "ConnectionHandler");
    }
}
