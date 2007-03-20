/* $Id: MpiPortType.java 5206 2007-03-14 08:53:14Z ceriel $ */

package ibis.impl.mpi;

import ibis.impl.Ibis;
import ibis.impl.PortType;
import ibis.impl.ReceivePort;
import ibis.impl.SendPort;
import ibis.ipl.IbisConfigurationException;
import ibis.ipl.ReceivePortConnectUpcall;
import ibis.ipl.SendPortDisconnectUpcall;
import ibis.ipl.CapabilitySet;
import ibis.ipl.Upcall;

import java.io.IOException;
import java.util.Properties;

class MpiPortType extends PortType {

    MpiPortType(Ibis ibis, CapabilitySet p, Properties tp) {
        super(ibis, p, tp);
        // if (serialization.equals("sun")) {
        //     throw new IbisConfigurationException("MpiIbis does not support "
        //             + "sun serialization");
        // }
    }

    protected SendPort doCreateSendPort(String nm, SendPortDisconnectUpcall cU,
            boolean connectionDowncalls) throws IOException {
        return new MpiSendPort(ibis, this, nm, connectionDowncalls, cU);
    }

    protected ReceivePort doCreateReceivePort(String nm, Upcall u,
            ReceivePortConnectUpcall cU, boolean connectionDowncalls)
            throws IOException {
        return new MpiReceivePort(ibis, this, nm, u, connectionDowncalls, cU);
    }
}
