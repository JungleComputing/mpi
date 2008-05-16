/* $Id:$ */

package ibis.impl.mpi;

import ibis.ipl.CapabilitySet;
import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.PortType;
import ibis.ipl.RegistryEventHandler;

import java.util.Properties;

import org.apache.log4j.Logger;

public final class MpiIbisStarter extends ibis.ipl.IbisStarter {

    static final Logger logger
            = Logger.getLogger(MpiIbisStarter.class);

    static final IbisCapabilities ibisCapabilities = new IbisCapabilities(
            IbisCapabilities.CLOSED_WORLD,
            IbisCapabilities.MEMBERSHIP_TOTALLY_ORDERED,
            IbisCapabilities.SIGNALS,
            IbisCapabilities.ELECTIONS_STRICT,
            "nickname.mpi"
        );

        static final PortType portCapabilities = new PortType(
            PortType.SERIALIZATION_OBJECT,
            PortType.SERIALIZATION_DATA,
            PortType.SERIALIZATION_BYTE,
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

    private boolean matching;
    private int unmatchedPortTypes;
    
    public MpiIbisStarter() {
    }

    public boolean matches(IbisCapabilities capabilities, PortType[] types) {
        this.capabilities = capabilities;
        this.portTypes = types.clone();
        matching = true;
        if (! capabilities.matchCapabilities(ibisCapabilities)) {
            matching = false;
        }
        for (PortType pt : portTypes) {
            if (! pt.matchCapabilities(portCapabilities)) {
                unmatchedPortTypes++;
                matching = false;
            }
        }
        return matching;
    }

    public boolean isSelectable() {
        return true;
    }

    public CapabilitySet unmatchedIbisCapabilities() {
        return capabilities.unmatchedCapabilities(ibisCapabilities);
    }

    public PortType[] unmatchedPortTypes() {
        PortType[] unmatched = new PortType[unmatchedPortTypes];
        int i = 0;
        for (PortType pt : portTypes) {
            if (! pt.matchCapabilities(portCapabilities)) {
                unmatched[i++] = pt;
            }
        }
        return unmatched;
    }

    public Ibis startIbis(RegistryEventHandler registryEventHandler,
            Properties userProperties) {
        return new MpiIbis(registryEventHandler, capabilities, portTypes,
                userProperties);
    }
}