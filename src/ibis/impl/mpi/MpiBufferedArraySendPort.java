/* $Id: MpiSendPort.java 5206 2007-03-14 08:53:14Z ceriel $ */

package ibis.impl.mpi;

import ibis.io.BufferedArrayOutputStream;
import ibis.io.OutputStreamSplitter;
import ibis.io.SplitterException;
import ibis.ipl.PortType;
import ibis.ipl.SendPortDisconnectUpcall;
import ibis.ipl.impl.Ibis;
import ibis.ipl.impl.ReceivePortIdentifier;
import ibis.ipl.impl.SendPortConnectionInfo;
import ibis.ipl.impl.WriteMessage;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

final class MpiBufferedArraySendPort extends MpiSendPort implements MpiProtocol {

    private class Conn extends SendPortConnectionInfo {
        OutputStream out;

        Conn(MpiBufferedArraySendPort port, ReceivePortIdentifier target,
                OutputStream out) throws IOException {
            super(port, target);
            this.out = out;
            splitter.add(out);
        }

        public void closeConnection() {
            try {
                splitter.remove(out);
            } catch (IOException e) {
                // ignored
            }
        }
    }

    final OutputStreamSplitter splitter;
    
    final BufferedArrayOutputStream bufferedStream;

    MpiBufferedArraySendPort(Ibis ibis, PortType type, String name,
            SendPortDisconnectUpcall cU, Properties props) throws IOException {
        super(ibis, type, name, cU, props);

        splitter = new OutputStreamSplitter(
                !type.hasCapability(PortType.CONNECTION_ONE_TO_ONE)
                && !type.hasCapability(
                    PortType.CONNECTION_MANY_TO_ONE),
                type.hasCapability(PortType.CONNECTION_ONE_TO_MANY)
                || type.hasCapability(
                    PortType.CONNECTION_MANY_TO_MANY));

        bufferedStream = new BufferedArrayOutputStream(splitter, 4096);
 
        initStream(bufferedStream);
    }
    
    protected long totalWritten() {
        return splitter.bytesWritten();
    }

    protected void resetWritten() {
        splitter.resetBytesWritten();
    }

    protected SendPortConnectionInfo doConnect(ReceivePortIdentifier receiver,
            long timeoutMillis, boolean fill) throws IOException {
        int myTag;
        synchronized(MpiSendPort.class) {
            myTag = tag++;
        }
        int rank = ((MpiIbis)ibis).connect(this, receiver, (int) timeoutMillis,
                myTag);
        Conn c = new Conn(this, receiver, new MpiDataOutputStream(rank, myTag));
        if (out != null) {
            out.writeByte(NEW_RECEIVER);
        }
        initStream(bufferedStream);
        return c;
    }
    
    protected synchronized void finishMessage(WriteMessage w, long cnt)
            throws IOException {
        if (multipleReceivers) {
            // exception may have been saved by the splitter. Get them
            // now.
            SplitterException e = splitter.getExceptions();
            if (e != null) {
                gotSendException(w, e);
            }
        }
        super.finishMessage(w, cnt);
    }

    protected void closePort() {
        super.closePort();
        try {
            bufferedStream.close();
        } catch (Throwable e) {
            // ignored
        }
    }
}
