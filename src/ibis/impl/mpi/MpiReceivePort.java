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

        public void close(Throwable e) {
            super.close(e);
        }

        public void run() {
            logger.debug("Started connection handler thread");
            try {
                reader();
            } catch (Throwable e) {
                logger.debug("ConnectionHandler.run, connected "
                        + "to " + origin + ", caught exception", e);
                close(e);
            }
        }

        protected void upcallCalledFinish() {
            super.upcallCalledFinish();
            ThreadPool.createNew(this, "ConnectionHandler");
        }

        void reader() throws IOException {
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
                    messageArrived(m);
                    // Note: if upcall calls finish, a new message is
                    // allocated, so we cannot look at "message" anymore.
                    if (m.finishCalledInUpcall()) {
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
                    }
                    break;
                default:
                    throw new IOException(name + ": Got illegal opcode "
                            + opcode + " from " + origin);
                }
            }
        }
    }

    MpiReceivePort(Ibis ibis, PortType type, String name, MessageUpcall upcall,
            ReceivePortConnectUpcall connUpcall, Properties props) throws IOException {
        super(ibis, type, name, upcall, connUpcall, props);
    }

    public void messageArrived(ReadMessage msg) {
        super.messageArrived(msg);
        if (upcall == null) {
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

    synchronized void connect(SendPortIdentifier origin, int tag, int rank)
            throws IOException {
        DataInputStream in = new MpiDataInputStream(rank, tag);
        ConnectionHandler conn = new ConnectionHandler(origin, this, in);

        ThreadPool.createNew(conn, "ConnectionHandler");
    }
}
