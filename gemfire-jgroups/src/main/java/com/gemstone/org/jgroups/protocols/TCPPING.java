/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: TCPPING.java,v 1.24 2005/08/11 12:43:47 belaban Exp $

package com.gemstone.org.jgroups.protocols;



import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicBoolean;

import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.util.ExternalStrings;


/**
 * The TCPPING protocol layer retrieves the initial membership in answer to the GMS's
 * FIND_INITIAL_MBRS event. The initial membership is retrieved by directly contacting other group
 * members, sending point-to-point mebership requests. The responses should allow us to determine
 * the coordinator whom we have to contact in case we want to join the group. When we are a server
 * (after having received the BECOME_SERVER event), we'll respond to TCPPING requests with a TCPPING
 * response.
 * <p>
 * The FIND_INITIAL_MBRS event will eventually be answered with a FIND_INITIAL_MBRS_OK event up
 * the stack.
 * <p>
 * The TCPPING protocol requires a static conifiguration, which assumes that you to know in advance
 * where to find other members of your group. For dynamic discovery, use the PING protocol, which
 * uses multicast discovery, or the TCPGOSSIP protocol, which contacts a Gossip Router to acquire
 * the initial membership.
 *
 * @author Bela Ban
 */
public class TCPPING extends Discovery  {
    int             port_range=1;        // number of ports to be probed for initial membership

    /** List of IpAddress */
    ArrayList       initial_hosts=null;  // hosts to be contacted for the initial membership
    final static String name="TCPPING";



    @Override // GemStoneAddition
    public String getName() {
        return name;
    }


    @Override // GemStoneAddition
    public boolean setProperties(Properties props) {
        String str;

        str=props.getProperty("port_range");           // if member cannot be contacted on base port,
        if(str != null) {                              // how many times can we increment the port
            port_range=Integer.parseInt(str);
            if (port_range < 1) {
               port_range = 1;    
            }
            props.remove("port_range");
        }

        str=props.getProperty("initial_hosts");
        if(str != null) {
            props.remove("initial_hosts");
            initial_hosts=createInitialHosts(str);
        }

        return super.setProperties(props);
    }


    @Override // GemStoneAddition
    public void localAddressSet(Address addr) {
        // Add own address to initial_hosts if not present: we must always be able to ping ourself !
        if(initial_hosts != null && addr != null) {
            if(initial_hosts.contains(addr)) {
                initial_hosts.remove(addr);
                if(log.isDebugEnabled()) log.debug("[SET_LOCAL_ADDRESS]: removing my own address (" + addr +
                                                   ") from initial_hosts; initial_hosts=" + initial_hosts);
            }
        }
    }


    @Override // GemStoneAddition
    public void sendGetMembersRequest(AtomicBoolean waiter_sync) {
        Message msg;

        wakeWaiter(waiter_sync);

        for(Iterator it=initial_hosts.iterator(); it.hasNext();) {
            Address addr=(Address)it.next();
            // if(tmpMbrs.contains(addr)) {
               // ; // continue; // changed as suggested by Mark Kopec
            // }
            msg=new Message(addr, null, null);
            msg.putHeader(name, new PingHeader(PingHeader.GET_MBRS_REQ, null));

            if(trace) log.trace("[FIND_INITIAL_MBRS] sending PING request to " + msg.getDest());
            passDown(new Event(Event.MSG, msg));
        }
    }



    /* -------------------------- Private methods ---------------------------- */

    /**
     * Input is "daddy[8880],sindhu[8880],camille[5555]. Return List of IpAddresses
     */
    private ArrayList createInitialHosts(String l) {
        StringTokenizer tok=new StringTokenizer(l, ",");
        String          t;
        IpAddress       addr;
        ArrayList       retval=new ArrayList();

        while(tok.hasMoreTokens()) {
            try {
                t=tok.nextToken();
                String host=t.substring(0, t.indexOf('['));
                int port=Integer.parseInt(t.substring(t.indexOf('[') + 1, t.indexOf(']')));
                for(int i=port; i < port + port_range; i++) {
                    addr=new IpAddress(host, i);
                    retval.add(addr);
                }
            }
            catch(NumberFormatException e) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.TCPPING_EXEPTION_IS__0, e);
            }
        }

        return retval;
    }

}

