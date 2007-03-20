/* $Id: MpiIbis.java 5175 2007-03-07 13:06:34Z ndrost $ */

package ibis.impl.mpi;

import ibis.impl.IbisIdentifier;
import ibis.impl.ReceivePort;
import ibis.impl.SendPortIdentifier;
import ibis.io.BufferedArrayInputStream;
import ibis.io.BufferedArrayOutputStream;
import ibis.ipl.AlreadyConnectedException;
import ibis.ipl.CapabilitySet;
import ibis.ipl.ConnectionRefusedException;
import ibis.ipl.ConnectionTimedOutException;
import ibis.ipl.PortMismatchException;
import ibis.ipl.ResizeHandler;
import ibis.util.IPUtils;
import ibis.util.ThreadPool;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Properties;

import org.apache.log4j.Logger;

public final class MpiIbis extends ibis.impl.Ibis
        implements Runnable, MpiProtocol {

    private static final Logger logger
            = Logger.getLogger("ibis.impl.mpi.MpiIbis");

    private IbisMPIInterface mpi;

    private ServerSocket systemServer;

    private InetSocketAddress myAddress;

    private InetSocketAddress local;

    private boolean quiting = false;

    private IbisIdentifier[] ibisIds;

    private InetSocketAddress[] addresses;

    private HashMap<IbisIdentifier, Integer> map
            = new HashMap<IbisIdentifier, Integer>();

    public MpiIbis(ResizeHandler r, CapabilitySet p, Properties tp)
        throws Throwable {

        super(r, p, tp, null);

        ThreadPool.createNew(this, "MpiIbis");
    }

    protected byte[] getData() throws IOException {
        InetAddress addr = IPUtils.getLocalHostAddress();
        if (addr == null) {
            logger.fatal("ERROR: could not get my own IP address, exiting.");
            System.exit(1);
        }

        mpi = IbisMPIInterface.createMpi(properties());

        int size = mpi.getSize();
        addresses = new InetSocketAddress[size];
        ibisIds = new IbisIdentifier[size];

        systemServer = new ServerSocket();
        local = new InetSocketAddress(addr, 0);
        systemServer.bind(local, 50);
        myAddress = new InetSocketAddress(addr, systemServer.getLocalPort());

        if (logger.isDebugEnabled()) {
            logger.debug("--> MpiIbis: address = " + myAddress);
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos);
        out.writeObject(myAddress);
        out.writeInt(mpi.getRank());
        out.close();

        return bos.toByteArray();
    }

    protected ibis.impl.PortType newPortType(CapabilitySet p, Properties tp) {
        return new MpiPortType(this, p, tp);
    }

    public void left(IbisIdentifier[] ids) {
        super.left(ids);
        synchronized(addresses) {
            for (int i = 0; i < ids.length; i++) {
                Integer n = map.get(ids[i]);
                if (n != null) {
                    addresses[n.intValue()] = null;
                    ibisIds[n.intValue()] = null;
                    map.remove(ids[i]);
                }
            }
        }
    }

    public void died(IbisIdentifier[] ids) {
        super.died(ids);
        synchronized(addresses) {
            for (int i = 0; i < ids.length; i++) {
                Integer n = map.get(ids[i]);
                if (n != null) {
                    addresses[n.intValue()] = null;
                    ibisIds[n.intValue()] = null;
                    map.remove(ids[i]);
                }
            }
        }
    }

    synchronized InetSocketAddress getAddress(IbisIdentifier id)
            throws IOException {
        Integer v = map.get(id);
        InetSocketAddress idAddr;
        if (v == null) {
            ObjectInputStream in = new ObjectInputStream(
                    new java.io.ByteArrayInputStream(
                            id.getImplementationData()));
            try {
                idAddr = (InetSocketAddress) in.readObject();
            } catch(ClassNotFoundException e) {
                throw new IOException("Could not get address from " + id);
            }
            int rank = in.readInt();
            in.close();
            addresses[rank] = idAddr;
            ibisIds[rank] = id;
            map.put(id, new Integer(rank));
        } else {
            idAddr = addresses[v.intValue()];
        }
        return idAddr;
    }

    int connect(MpiSendPort sp, ibis.impl.ReceivePortIdentifier rip,
            int timeout, int tag) throws IOException {
        IbisIdentifier id = (IbisIdentifier) rip.ibis();
        String name = rip.name();
        InetSocketAddress idAddr = getAddress(id);

        int port = idAddr.getPort();

        long startTime = System.currentTimeMillis();

        if (logger.isDebugEnabled()) {
            logger.debug("--> Creating socket for connection to " + name
                    + " at " + id + ", port = " + port);
        }

        do {
            DataOutputStream out = null;
            InputStream in = null;
            Socket s = null;
            int result = -1;

            try {
                s = createClientSocket(local, idAddr, timeout);
                out = new DataOutputStream(new BufferedArrayOutputStream(
                            s.getOutputStream()));

                out.writeUTF(name);
                sp.getIdent().writeTo(out);
                sp.getType().capabilities().writeTo(out);
                out.writeInt(tag);
                out.flush();

                result = s.getInputStream().read();

                switch(result) {
                case ReceivePort.ACCEPTED:
                    return map.get(id).intValue();
                case ReceivePort.ALREADY_CONNECTED:
                    throw new AlreadyConnectedException(
                            "The sender was already connected to " + name
                            + " at " + id);
                case ReceivePort.TYPE_MISMATCH:
                    throw new PortMismatchException(
                            "Cannot connect ports of different PortTypes");
                case ReceivePort.DENIED:
                    throw new ConnectionRefusedException(
                            "Receiver denied connection");
                case ReceivePort.NO_MANYTOONE:
                    throw new ConnectionRefusedException(
                            "Receiver already has a connection and ManyToOne "
                            + "is not set");
                case ReceivePort.NOT_PRESENT:
                case ReceivePort.DISABLED:
                    // and try again if we did not reach the timeout...
                    if (timeout > 0 && System.currentTimeMillis()
                            > startTime + timeout) {
                        throw new ConnectionTimedOutException(
                                "Could not connect");
                    }
                    break;
                default:
                    throw new Error("Illegal opcode in MpiIbis.connect");
                }
            } catch(SocketTimeoutException e) {
                throw new ConnectionTimedOutException("Could not connect");
            } finally {
                try {
                    out.close();
                } catch(Throwable e) {
                    // ignored
                }
                try {
                    s.close();
                } catch(Throwable e) {
                    // ignored
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // ignore
            }
        } while (true);
    }

    protected void quit() {
        try {
            quiting = true;
            // Connect so that the MpiIbis thread wakes up.
            createClientSocket(local, myAddress, 0);
        } catch (Throwable e) {
            // Ignore
        }
    }

    private void handleConnectionRequest(Socket s) throws IOException {
        if (logger.isDebugEnabled()) {
            logger.debug("--> MpiIbis got connection request from "
                    + s.getInetAddress() + ":" + s.getPort() + " on local port "
                    + s.getLocalPort());
        }

        BufferedArrayInputStream bais
                = new BufferedArrayInputStream(s.getInputStream());

        DataInputStream in = new DataInputStream(bais);
        OutputStream out = s.getOutputStream();

        String name = in.readUTF();
        SendPortIdentifier send = new SendPortIdentifier(in);
        CapabilitySet sp = new CapabilitySet(in);
        int tag = in.readInt();

        // First, lookup receiveport.
        MpiReceivePort rp = (MpiReceivePort) findReceivePort(name);

        int result;
        if (rp == null) {
            result = ReceivePort.NOT_PRESENT;
        } else {
            result = rp.connectionAllowed(send, sp);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("--> S RP = " + name + ": "
                    + ReceivePort.getString(result));
        }

        out.write(result);
        out.close();
        in.close();
        if (result == ReceivePort.ACCEPTED) {
            // add the connection to the receiveport.
            getAddress(send.ibis);
            rp.connect(send, tag, map.get(send.ibis).intValue());
            if (logger.isDebugEnabled()) {
                logger.debug("--> S connect done ");
            }
        }
    }

    private Socket createClientSocket(InetSocketAddress local,
            InetSocketAddress remote, int timeout) throws IOException {
        Socket s = new Socket();
        s.bind(local);
        s.connect(remote, timeout);
        return s;
    }

    public void run() {
        // This thread handles incoming connection request from the
        // connect(MpiSendPort) call.
        while (true) {
            Socket s = null;

            if (logger.isDebugEnabled()) {
                logger.debug("--> MpiIbis doing new accept()");
            }
            try {
                s = systemServer.accept();
            } catch (Throwable e) {
                /* if the accept itself fails, we have a fatal problem. */
                logger.fatal("MpiIbis:run: got fatal exception in accept! ", e);
                cleanup();
                throw new Error("Fatal: MpiIbis could not do an accept", e);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("--> MpiIbis through new accept()");
            }
            try {
                if (quiting) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("--> it is a quit: RETURN");
                    }
                    cleanup();
                    return;
                }
                handleConnectionRequest(s);
            } catch (Throwable e) {
                logger.error("EEK: MpiIbis:run: got exception "
                        + "(closing this socket only: ", e);
            } finally {
                try {
                    s.close();
                } catch(Throwable e) {
                    // ignored
                }
            }
        }
    }

    private void cleanup() {
        try {
            if (systemServer != null) {
                systemServer.close();
                systemServer = null;
            }
        } catch (Throwable e) {
            // Ignore
        }
    }
}
