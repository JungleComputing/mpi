/* $Id: MpiIbis.java 5175 2007-03-07 13:06:34Z ndrost $ */

package ibis.impl.mpi;

import ibis.io.BufferedArrayInputStream;
import ibis.io.BufferedArrayOutputStream;
import ibis.ipl.AlreadyConnectedException;
import ibis.ipl.ConnectionRefusedException;
import ibis.ipl.ConnectionTimedOutException;
import ibis.ipl.Credentials;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisStarter;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortMismatchException;
import ibis.ipl.PortType;
import ibis.ipl.ReceivePortConnectUpcall;
import ibis.ipl.RegistryEventHandler;
import ibis.ipl.SendPortDisconnectUpcall;
import ibis.ipl.impl.IbisIdentifier;
import ibis.ipl.impl.ReceivePort;
import ibis.ipl.impl.SendPortIdentifier;
import ibis.util.IPUtils;
import ibis.util.ThreadPool;
import ibis.util.TypedProperties;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MpiIbis extends ibis.ipl.impl.Ibis
        implements Runnable, MpiProtocol {
    
    private static final int CONNECT = 1;
    private static final int DISCONNECT = 2;

    private static final Logger logger
            = LoggerFactory.getLogger("ibis.impl.mpi.MpiIbis");

    private IbisMPIInterface mpi;

    private ServerSocket systemServer;

    private InetSocketAddress myAddress;

    private InetSocketAddress local;

    private boolean quiting = false;

    private IbisIdentifier[] ibisIds;

    private InetSocketAddress[] addresses;
    
    private HashMap<IbisIdentifier, Integer> map
            = new HashMap<IbisIdentifier, Integer>();
    
    final boolean useBufferedArrayStreams;
    
    public MpiIbis(RegistryEventHandler registryEventHandler,
            IbisCapabilities capabilities, Credentials credentials,
            byte[] tag, PortType[] types, Properties userProperties, IbisStarter starter) {
        super(registryEventHandler, capabilities, credentials, tag, types,
                userProperties, starter);
        TypedProperties properties = new TypedProperties(properties());
        useBufferedArrayStreams
                = properties.getBooleanProperty("ibis.mpi.bufferedStreams", false);
        ThreadPool.createNew(this, "MpiIbis");
    }
    
    protected byte[] getData() throws IOException {
        InetAddress addr = IPUtils.getLocalHostAddress();
        if (addr == null) {
            logger.error("ERROR: could not get my own IP address, exiting.");
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
    
    void sendDisconnect(ibis.ipl.impl.ReceivePortIdentifier rip) {
        IbisIdentifier id = (IbisIdentifier) rip.ibisIdentifier();
        String name = rip.name();
        
        InetSocketAddress idAddr = addresses[map.get(id)];
        int port = idAddr.getPort();

        if (logger.isDebugEnabled()) {
            logger.debug("--> Creating socket for connection to " + name
                    + " at " + id + ", port = " + port);
        }

        DataOutputStream out = null;
        Socket s = null;
 
        try {
            s = createClientSocket(local, idAddr, 1000);
            out = new DataOutputStream(new BufferedArrayOutputStream(
                    s.getOutputStream(), 4096));
            out.writeInt(DISCONNECT);
            out.writeUTF(name);
            out.flush();
            s.getInputStream().read();
        } catch(IOException e) {
            logger.info("Could not send disconnect message", e);
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch(Throwable e) {
                // ignored
            }
            try {
                if (s != null) {
                    s.close();
                }
            } catch(Throwable e) {
                // ignored
            }
        }
    }

    int connect(MpiSendPort sp, ibis.ipl.impl.ReceivePortIdentifier rip,
            int timeout, int tag) throws IOException {
        IbisIdentifier id = (IbisIdentifier) rip.ibisIdentifier();
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
            Socket s = null;
            int result = -1;

            try {
                s = createClientSocket(local, idAddr, timeout);
                out = new DataOutputStream(new BufferedArrayOutputStream(
                            s.getOutputStream(), 4096));
                out.writeInt(CONNECT);
                out.writeUTF(name);
                sp.getIdent().writeTo(out);
                sp.getPortType().writeTo(out);
                out.writeInt(tag);
                out.flush();

                result = s.getInputStream().read();

                switch(result) {
                case ReceivePort.ACCEPTED:
                    return map.get(id).intValue();
                case ReceivePort.ALREADY_CONNECTED:
                    throw new AlreadyConnectedException(
                            "The sender was already connected", rip);
                case ReceivePort.TYPE_MISMATCH:
                    throw new PortMismatchException(
                            "Cannot connect ports of different port types", rip);
                case ReceivePort.DENIED:
                    throw new ConnectionRefusedException(
                            "Receiver denied connection", rip);
                case ReceivePort.NO_MANY_TO_X:
                    throw new ConnectionRefusedException(
                            "Receiver already has a connection and ManyToOne/ManyToMany "
                            + "is not set", rip);
                case ReceivePort.NOT_PRESENT:
                case ReceivePort.DISABLED:
                    // and try again if we did not reach the timeout...
                    if (timeout > 0 && System.currentTimeMillis()
                            > startTime + timeout) {
                        throw new ConnectionTimedOutException(
                                "Could not connect", rip);
                    }
                    break;
                default:
                    throw new Error("Illegal opcode in MpiIbis.connect");
                }
            } catch(SocketTimeoutException e) {
                throw new ConnectionTimedOutException("Could not connect", rip);
            } finally {
                try {
                    if (out != null) {
                        out.close();
                    }
                } catch(Throwable e) {
                    // ignored
                }
                try {
                    if (s != null) {
                        s.close();
                    }
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
            synchronized(this) {
                while (quiting) {
                    try {
                        wait();
                    } catch(Throwable e) {
                        // ignored
                    }
                }
            }
        } catch (Throwable e) {
            // Ignore
        }
    }

    private void handleRequest(Socket s) throws IOException {
        if (logger.isDebugEnabled()) {
            logger.debug("--> MpiIbis got request from "
                    + s.getInetAddress() + ":" + s.getPort() + " on local port "
                    + s.getLocalPort());
        }

        BufferedArrayInputStream bais
                = new BufferedArrayInputStream(s.getInputStream(), 4096);

        DataInputStream in = new DataInputStream(bais);
        OutputStream out = s.getOutputStream();
        
        int request = in.readInt();
        String name = in.readUTF();

        // First, lookup receiveport.
        MpiReceivePort rp = (MpiReceivePort) findReceivePort(name);

        if (request == DISCONNECT) {
            if (rp != null) {
                rp.addDisconnect();
            }
            out.write(1);
            out.close();
            in.close();
            return;
        }

        SendPortIdentifier send = new SendPortIdentifier(in);
        PortType sp = new PortType(in);
        int tag = in.readInt();

        int result;
        if (rp == null) {
            result = ReceivePort.NOT_PRESENT;
        } else {
            rp.waitForDisconnects();
            result = rp.connectionAllowed(send, sp);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("--> S RP = " + name + ": "
                    + ReceivePort.getString(result));
        }

        out.write(result);
        out.close();
        in.close();
        if (result == ReceivePort.ACCEPTED && rp != null) {
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
                logger.error("MpiIbis:run: got fatal exception in accept! ", e);
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
                    mpi.doEnd();
                    synchronized(this) {
                        quiting = false;
                        notifyAll();
                    }
                    return;
                }
                handleRequest(s);
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

    protected ibis.ipl.SendPort doCreateSendPort(PortType tp, String nm,
            SendPortDisconnectUpcall cU, Properties props)
            throws IOException {
        if (useBufferedArrayStreams) {
            return new MpiBufferedArraySendPort(this, tp, nm, cU, props);
        }
        return new MpiSendPort(this, tp, nm, cU, props);
    }

    protected ibis.ipl.ReceivePort doCreateReceivePort(PortType tp,
            String nm, MessageUpcall u, ReceivePortConnectUpcall cU, Properties props) throws IOException {
        return new MpiReceivePort(this, tp, nm, u, cU, props);
    }
}
