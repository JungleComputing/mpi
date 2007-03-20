/* $Id: MpiProtocol.java 4989 2007-01-25 17:39:22Z ceriel $ */

package ibis.impl.mpi;

interface MpiProtocol {
    static final byte NEW_RECEIVER = 1;

    static final byte NEW_MESSAGE = 2;

    static final byte CLOSE_ALL_CONNECTIONS = 3;

    static final byte CLOSE_ONE_CONNECTION = 4;

    static final byte QUIT_IBIS = 14;

    static final byte REPLY = 127;
}
