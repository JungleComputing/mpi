/* $Id: MpiIbis.java 5175 2007-03-07 13:06:34Z ndrost $ */

package ibis.impl.mpi;

import ibis.ipl.AlreadyConnectedException;
import ibis.ipl.ConnectionRefusedException;
import ibis.ipl.ConnectionTimedOutException;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisConfigurationException;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortMismatchException;
import ibis.ipl.PortType;
import ibis.ipl.ReceivePortConnectUpcall;
import ibis.ipl.impl.Registry;
import ibis.ipl.RegistryEventHandler;
import ibis.ipl.SendPortDisconnectUpcall;
import ibis.ipl.impl.IbisIdentifier;
import ibis.ipl.impl.ReceivePort;
import ibis.ipl.impl.SendPortIdentifier;
import ibis.util.IPUtils;
import ibis.util.ThreadPool;
import ibis.util.io.BufferedArrayInputStream;
import ibis.util.io.BufferedArrayOutputStream;

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

import org.apache.log4j.Logger;

public final class MpiIbis extends ibis.ipl.impl.Ibis
        implements RegistryEventHandler, Runnable, MpiProtocol {

    static final IbisCapabilities ibisCapabilities = new IbisCapabilities(
            IbisCapabilities.WORLDMODEL_OPEN,
            IbisCapabilities.WORLDMODEL_CLOSED,
            IbisCapabilities.REGISTRY_DOWNCALLS,
            IbisCapabilities.REGISTRY_UPCALLS,
            "nickname.mpi"
        );

        static final PortType portCapabilities = new PortType(
            PortType.SERIALIZATION_OBJECT,
            PortType.SERIALIZATION_DATA,
            PortType.SERIALIZATION_BYTE,
            PortType.SERIALIZATION_REPLACER + "=*",
            PortType.COMMUNICATION_FIFO,
            PortType.COMMUNICATION_NUMBERED,
            PortType.COMMUNICATION_RELIABLE,
            PortType.CONNECTION_DOWNCALLS,
            PortType.CONNECTION_UPCALLS,
            PortType.CONNECTION_TIMEOUT,
            PortType.CONNECTION_MANY_TO_ONE,
            PortType.CONNECTION_ONE_TO_MANY,
            PortType.CONNECTION_ONE_TO_ONE,
            PortType.RECEIVE_POLL,
            PortType.RECEIVE_AUTO_UPCALLS,
            PortType.RECEIVE_EXPLICIT,
            PortType.RECEIVE_POLL_UPCALLS,
            PortType.RECEIVE_TIMEOUT
        );

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
    
    private RegistryEventHandler eventHandler;

    public MpiIbis(RegistryEventHandler r, IbisCapabilities p, PortType[] types, Properties tp)
        throws Throwable {

        super(r, p, types, tp, null);
        ThreadPool.createNew(this, "MpiIbis");
    }
    
    protected Registry initializeRegistry(RegistryEventHandler handler, 
            IbisCapabilities caps) {
        eventHandler = handler;
        try {
            return Registry.createRegistry(caps, this, properties, 
                    getData());
        } catch(Throwable e) {
            throw new IbisConfigurationException("Could not create registry",
                    e);
        }
    }
    
    protected PortType getPortCapabilities() {
        return portCapabilities;
    }
    
    protected IbisCapabilities getCapabilities() {
        return ibisCapabilities;
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
    
    public void joined(ibis.ipl.IbisIdentifier id) {
        if (eventHandler != null) {
            eventHandler.joined(id);
        }
    }

    public void gotSignal(String sig) {
        if (eventHandler != null) {
            eventHandler.gotSignal(sig);
        }
    }
    
    public void left(ibis.ipl.IbisIdentifier id) {
        if (eventHandler != null) {
            eventHandler.left(id);
        }
        synchronized(addresses) {
            Integer n = map.get(id);
            if (n != null) {
                addresses[n.intValue()] = null;
                ibisIds[n.intValue()] = null;
                map.remove(id);
            }
        }
    }

    public void died(ibis.ipl.IbisIdentifier id) {
        if (eventHandler != null) {
            eventHandler.died(id);
        }
        synchronized(addresses) {
            Integer n = map.get(id);
            if (n != null) {
                addresses[n.intValue()] = null;
                ibisIds[n.intValue()] = null;
                map.remove(id);
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
                case ReceivePort.NO_MANYTOONE:
                    throw new ConnectionRefusedException(
                            "Receiver already has a connection and ManyToOne "
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
                = new BufferedArrayInputStream(s.getInputStream(), 4096);

        DataInputStream in = new DataInputStream(bais);
        OutputStream out = s.getOutputStream();

        String name = in.readUTF();
        SendPortIdentifier send = new SendPortIdentifier(in);
        PortType sp = new PortType(in);
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

    protected ibis.ipl.SendPort doCreateSendPort(PortType tp, String nm,
            SendPortDisconnectUpcall cU, Properties props)
            throws IOException {
        return new MpiSendPort(this, tp, nm, cU, props);
    }

    protected ibis.ipl.ReceivePort doCreateReceivePort(PortType tp,
            String nm, MessageUpcall u, ReceivePortConnectUpcall cU, Properties props) throws IOException {
        return new MpiReceivePort(this, tp, nm, u, cU, props);
    }
}
