/* $Id: MpiSendPort.java 5206 2007-03-14 08:53:14Z ceriel $ */

package ibis.impl.mpi;

import ibis.ipl.PortType;
import ibis.ipl.SendPortDisconnectUpcall;
import ibis.ipl.impl.Ibis;
import ibis.ipl.impl.ReceivePortIdentifier;
import ibis.ipl.impl.SendPort;
import ibis.ipl.impl.SendPortConnectionInfo;
import ibis.ipl.impl.SendPortIdentifier;
import ibis.ipl.impl.WriteMessage;
import ibis.util.io.Conversion;
import ibis.util.io.DataOutputStream;
import ibis.util.io.DataOutputStreamSplitter;
import ibis.util.io.SplitterException;

import java.io.IOException;
import java.io.OutputStream;

final class MpiSendPort extends SendPort implements MpiProtocol {

    private class Conn extends SendPortConnectionInfo {
        DataOutputStream out;

        Conn(MpiSendPort port, ReceivePortIdentifier target,
                DataOutputStream out) throws IOException {
            super(port, target);
            this.out = out;
            splitter.add(out);
        }

        public void closeConnection() {
            splitter.remove(out);
        }
    }

    final DataOutputStreamSplitter splitter;

    private static int tag = 0;

    MpiSendPort(Ibis ibis, PortType type, String name,
            SendPortDisconnectUpcall cU) throws IOException {
        super(ibis, type, name, cU);

        splitter = new DataOutputStreamSplitter(
                ! type.hasCapability(PortType.CONNECTION_ONE_TO_ONE)
                && ! type.hasCapability(PortType.CONNECTION_MANY_TO_ONE));
 
        initStream(splitter);
    }

    SendPortIdentifier getIdent() {
        return ident;
    }

    protected SendPortConnectionInfo doConnect(ReceivePortIdentifier receiver,
            long timeoutMillis) throws IOException {
        int myTag;
        synchronized(this.getClass()) {
            myTag = tag++;
        }
        int rank = ((MpiIbis)ibis).connect(this, receiver, (int) timeoutMillis,
                myTag);
        Conn c = new Conn(this, receiver, new MpiDataOutputStream(rank, myTag));
        if (out != null) {
            out.writeByte(NEW_RECEIVER);
        }
        initStream(splitter);
        return c;
    }

    protected void disconnectPort(ReceivePortIdentifier receiver,
            SendPortConnectionInfo conn) throws IOException {

        out.writeByte(CLOSE_ONE_CONNECTION);

        byte[] receiverBytes = receiver.toBytes();
        byte[] receiverLength = new byte[Conversion.INT_SIZE];
        Conversion.defaultConversion.int2byte(receiverBytes.length,
            receiverLength, 0);
        out.writeArray(receiverLength);
        out.writeArray(receiverBytes);
        out.flush();
    }

    protected void announceNewMessage() throws IOException {
        out.writeByte(NEW_MESSAGE);
        if (type.hasCapability(PortType.COMMUNICATION_NUMBERED)) {
            out.writeLong(ibis.registry().getSeqno(name));
        }
    }

    protected void handleSendException(WriteMessage w, IOException e) {
        if (e instanceof SplitterException) {
            forwardLosses((SplitterException) e);
        }
    }

    private void forwardLosses(SplitterException e) {
        ReceivePortIdentifier[] ports = receivers.keySet().toArray(
                new ReceivePortIdentifier[0]);
        Exception[] exceptions = e.getExceptions();
        OutputStream[] streams = e.getStreams();

        for (int i = 0; i < ports.length; i++) {
            Conn c = (Conn) getInfo(ports[i]);
            for (int j = 0; j < streams.length; j++) {
                if (c.out == streams[j]) {
                    lostConnection(ports[i], exceptions[j]);
                    break;
                }
            }
        }
    }

    protected void closePort() {
        try {
            out.writeByte(CLOSE_ALL_CONNECTIONS);
            out.close();
        } catch (Throwable e) {
            // ignored
        }

        out = null;
    }
}
