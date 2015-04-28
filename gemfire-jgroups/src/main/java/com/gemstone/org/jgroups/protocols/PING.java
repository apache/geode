/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: PING.java,v 1.27 2005/08/11 12:43:47 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import java.net.InetAddress;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.stack.GossipClient;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.List;
import com.gemstone.org.jgroups.util.Util;


/**
 * The PING protocol layer retrieves the initial membership (used by the GMS when started
 * by sending event FIND_INITIAL_MBRS down the stack). We do this by mcasting PING
 * requests to an IP MCAST address (or, if gossiping is enabled, by contacting the GossipServer).
 * The responses should allow us to determine the coordinator whom we have to
 * contact, e.g. in case we want to join the group.  When we are a server (after having
 * received the BECOME_SERVER event), we'll respond to PING requests with a PING
 * response.<p> The FIND_INITIAL_MBRS event will eventually be answered with a
 * FIND_INITIAL_MBRS_OK event up the stack.
 * The following properties are available
 * property: gossip_host - if you are using GOSSIP then this defines the host of the GossipServer, default is null
 * property: gossip_port - if you are using GOSSIP then this defines the port of the GossipServer, default is null
 */
public class PING extends Discovery  {
    String       gossip_host=null;
    int          gossip_port=0;
    long         gossip_refresh=20000; // time in msecs after which the entry in GossipServer will be refreshed
    GossipClient client;
    int          port_range=1;        // number of ports to be probed for initial membership
    List         initial_hosts=null;  // hosts to be contacted for the initial membership
    public static final String name="PING";


    @Override // GemStoneAddition
    public String getName() {
        return name;
    }

    // start GemStoneAddition
    @Override // GemStoneAddition
    public int getProtocolEnum() {
      return com.gemstone.org.jgroups.stack.Protocol.enumPING;
    }
    // end GemStone addition



    /**
     * sets the properties of the PING protocol.
     * The following properties are available
     * property: timeout - the timeout (ms) to wait for the initial members, default is 3000=3 secs
     * property: num_initial_members - the minimum number of initial members for a FIND_INITAL_MBRS, default is 2
     * property: gossip_host - if you are using GOSSIP then this defines the host of the GossipServer, default is null
     * property: gossip_port - if you are using GOSSIP then this defines the port of the GossipServer, default is null
     *
     * @param props - a property set containing only PING properties
     * @return returns true if all properties were parsed properly
     *         returns false if there are unrecnogized properties in the property set
     */
    @Override // GemStoneAddition
    public boolean setProperties(Properties props) {
        String str;

        str=props.getProperty("gossip_host");
        if(str != null) {
            gossip_host=str;
            props.remove("gossip_host");
        }

        str=props.getProperty("gossip_port");
        if(str != null) {
            gossip_port=Integer.parseInt(str);
            props.remove("gossip_port");
        }

        str=props.getProperty("gossip_refresh");
        if(str != null) {
            gossip_refresh=Long.parseLong(str);
            props.remove("gossip_refresh");
        }

        if(gossip_host != null && gossip_port != 0) {
            try {
                client=new GossipClient(new IpAddress(InetAddress.getByName(gossip_host), gossip_port), gossip_refresh);
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.PING_CREATION_OF_GOSSIPCLIENT_FAILED_EXCEPTION_0, e);
                return false; // will cause stack creation to abort
            }
        }

        str=props.getProperty("port_range");           // if member cannot be contacted on base port,
        if(str != null) {                              // how many times can we increment the port
            port_range=Integer.parseInt(str);
            if(port_range < 1) {
                port_range=1;
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
    public void stop() {
        super.stop();
        if(client != null) {
            client.stop();
        }
    }


    @Override // GemStoneAddition
    public void localAddressSet(Address addr) {
        // Add own address to initial_hosts if not present: we must always be able to ping ourself !
        if(initial_hosts != null && local_addr != null) {
            List hlist;
            boolean inInitialHosts=false;
            for(Enumeration en=initial_hosts.elements(); en.hasMoreElements() && !inInitialHosts;) {
                hlist=(List)en.nextElement();
                if(hlist.contains(local_addr)) {
                    inInitialHosts=true;
                }
            }
            if(!inInitialHosts) {
                hlist=new List();
                hlist.add(local_addr);
                initial_hosts.add(hlist);
                if(log.isDebugEnabled())
                    log.debug("adding my address (" + local_addr + ") to initial_hosts; initial_hosts=" + initial_hosts);
            }
        }
    }


    @Override // GemStoneAddition
    public void handleConnect() {
        if(client != null)
            client.register(group_addr, local_addr, 0, false);
    }

    @Override // GemStoneAddition
    public void handleDisconnect() {
        if(client != null)
            client.stop();
    }



    @Override // GemStoneAddition
    public void sendGetMembersRequest(AtomicBoolean waiter_sync) {
        Message msg;
        PingHeader hdr;
        Vector gossip_rsps;

        wakeWaiter(waiter_sync);

        if(client != null) {
            gossip_rsps=client.getMembers(group_addr, local_addr, true, 0); // GemStoneAddition - localaddr
            if(gossip_rsps != null && gossip_rsps.size() > 0) {
                // Set a temporary membership in the UDP layer, so that the following multicast
                // will be sent to all of them
                Event view_event=new Event(Event.TMP_VIEW, makeView(gossip_rsps));
                passDown(view_event); // needed e.g. by failure detector or UDP
            }
            else {
                passUp(new Event(Event.FIND_INITIAL_MBRS_OK, null));
                return;
            }

            if(gossip_rsps.size() > 0) {
                for(int i=0; i < gossip_rsps.size(); i++) {
                    Address dest=(Address)gossip_rsps.elementAt(i);
                    msg=new Message(dest, null, null);
                    msg.putHeader(getName(), new PingHeader(PingHeader.GET_MBRS_REQ, null));
                    passDown(new Event(Event.MSG, msg));
                }
            }

            try { // GemStoneAddition
            Util.sleep(500);
            }
            catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              // We're done in this method; just propagate the thread bit
            }
        }
        else {
            if(initial_hosts != null && initial_hosts.size() > 0) {
                IpAddress h;
                List hlist;
                msg=new Message(null, null, null);
                msg.putHeader(getName(), new PingHeader(PingHeader.GET_MBRS_REQ, null));
                for(Enumeration en=initial_hosts.elements(); en.hasMoreElements();) {
                    hlist=(List)en.nextElement();
                    boolean isMember=false;
                    for(Enumeration hen=hlist.elements(); hen.hasMoreElements() && !isMember;) {
                        h=(IpAddress)hen.nextElement();
                        msg.setDest(h);
                        if(trace)
                            log.trace("[FIND_INITIAL_MBRS] sending PING request to " + msg.getDest());
                        passDown(new Event(Event.MSG, msg.copy()));
                    }
                }
            }
            else {
                // 1. Mcast GET_MBRS_REQ message
                ping_waiter.clearResponses(); // GemStoneAddition - ignore past work
                hdr=new PingHeader(PingHeader.GET_MBRS_REQ, null);
                msg=new Message(null, null, null);  // mcast msg
                msg.putHeader(getName(), hdr); // needs to be getName(), so we might get "MPING" !
                try {
                  sendMcastDiscoveryRequest(msg);
                }
                catch (InterruptedException ie) {
                  Thread.currentThread().interrupt();
                  return;
                }
            }
        }
    }

    void sendMcastDiscoveryRequest(Message discovery_request) throws InterruptedException {
      // waiting for a random amount of time here helps avoid connection exceptions during
      // concurrent multicast startup
      if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
      long waitTime = new Random(this.hashCode()).nextInt(150);
      if (waitTime > 0) {
        Thread.sleep(waitTime);
      }
        passDown(new Event(Event.MSG, discovery_request));
    }

//    // GemStoneAddition - let others know if this process becomes the coordinator
//    public void down(Event evt) {
//      super.down(evt);
//      if (evt.getType() == Event.BECOME_SERVER) {
//        sendIAMCoordinator();
//      }
//    }

    /* -------------------------- Private methods ---------------------------- */



    /**
     * Input is "daddy[8880],sindhu[8880],camille[5555]. Return List of IpAddresses
     */
    private List createInitialHosts(String l) {
        List tmp=new List();
        StringTokenizer tok=new StringTokenizer(l, ",");
        String t;

        while(tok.hasMoreTokens()) {
            try {
                t=tok.nextToken();
                String host=t.substring(0, t.indexOf('['));
                int port=Integer.parseInt(t.substring(t.indexOf('[') + 1, t.indexOf(']')));
                List hosts=new List();
                for(int i=port; i < port + port_range; i++) {
                    hosts.add(new IpAddress(host, i));
                }
                tmp.add(hosts);
            }
            catch(NumberFormatException e) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.PING_EXEPTION_IS__0, e);
            }
        }
        return tmp;
    }


}
