/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/

package com.gemstone.org.jgroups.protocols;


import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.ViewId;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.ExternalStrings;


/**
 * The Discovery protocol layer retrieves the initial membership (used by the GMS when started
 * by sending event FIND_INITIAL_MBRS down the stack). We do this by specific subclasses, e.g. by mcasting PING
 * requests to an IP MCAST address or, if gossiping is enabled, by contacting the GossipServer.
 * The responses should allow us to determine the coordinator whom we have to
 * contact, e.g. in case we want to join the group.  When we are a server (after having
 * received the BECOME_SERVER event), we'll respond to PING requests with a PING
 * response.<p> The FIND_INITIAL_MBRS event will eventually be answered with a
 * FIND_INITIAL_MBRS_OK event up the stack.
 * The following properties are available
 * <ul>
 * <li>timeout - the timeout (ms) to wait for the initial members, default is 3000=3 secs
 * <li>num_initial_members - the minimum number of initial members for a FIND_INITAL_MBRS, default is 2
 * <li>num_ping_requests - the number of GET_MBRS_REQ messages to be sent (min=1), distributed over timeout ms
 * </ul>
 * @author Bela Ban
 * @version $Id: Discovery.java,v 1.14 2005/12/16 16:02:43 belaban Exp $
 */
public abstract class Discovery extends Protocol  {
    final Vector  members=new Vector(11);
    Address       local_addr=null;
    String        group_addr=null;
    long          timeout=3000;
    int           num_initial_members=2;
    boolean       is_server=false;
    PingWaiter    ping_waiter;


    /** Number of GET_MBRS_REQ messages to be sent (min=1), distributed over timeout ms */
    int           num_ping_requests=2;

    int           num_discovery_requests=0;

    // start GemStoneAddition
    @Override // GemStoneAddition
    public int getProtocolEnum() {
      return com.gemstone.org.jgroups.stack.Protocol.enumDISCOVERY;
    }
    // end GemStone addition
    
    /** Called after local_addr was set */
    public void localAddressSet(Address addr) {
    }

    // GemStoneAddition - parameters
    public abstract void sendGetMembersRequest(AtomicBoolean waiter_sync);


    /** Called when CONNECT_OK has been received */
    public void handleConnectOK() {
    }

    public void handleDisconnect() {
    }

    public void handleConnect() {
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout=timeout;
        if(ping_waiter != null)
            ping_waiter.setTimeout(timeout);
    }

    public int getNumInitialMembers() {
        return num_initial_members;
    }

    public void setNumInitialMembers(int num_initial_members) {
        this.num_initial_members=num_initial_members;
        if(ping_waiter != null)
            ping_waiter.setNumRsps(num_initial_members);
    }

    public int getNumPingRequests() {
        return num_ping_requests;
    }

    public void setNumPingRequests(int num_ping_requests) {
        this.num_ping_requests=num_ping_requests;
    }

    public int getNumberOfDiscoveryRequestsSent() {
        return num_discovery_requests;
    }


    @Override // GemStoneAddition
    public Vector providedUpServices() {
        Vector ret=new Vector(1);
        ret.addElement(Integer.valueOf(Event.FIND_INITIAL_MBRS));
        return ret;
    }

    /**
     * sets the properties of the PING protocol.
     * The following properties are available
     * property: timeout - the timeout (ms) to wait for the initial members, default is 3000=3 secs
     * property: num_initial_members - the minimum number of initial members for a FIND_INITAL_MBRS, default is 2
     * @param props - a property set
     * @return returns true if all properties were parsed properly
     *         returns false if there are unrecnogized properties in the property set
     */
    @Override // GemStoneAddition
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("timeout");              // max time to wait for initial members
        if(str != null) {
            timeout=Long.parseLong(str);
            if(timeout <= 0) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.Discovery_TIMEOUT_MUST_BE__0);
                return false;
            }
            props.remove("timeout");
        }

        str=props.getProperty("num_initial_members");  // wait for at most n members
        if(str != null) {
            num_initial_members=Integer.parseInt(str);
            props.remove("num_initial_members");
        }

        str=props.getProperty("num_ping_requests");  // number of GET_MBRS_REQ messages
        if(str != null) {
            num_ping_requests=Integer.parseInt(str);
            props.remove("num_ping_requests");
            if(num_ping_requests < 1)
                num_ping_requests=1;
        }

        if(props.size() > 0) {
            StringBuffer sb=new StringBuffer();
            for(Enumeration e=props.propertyNames(); e.hasMoreElements();) {
                sb.append(e.nextElement().toString());
                if(e.hasMoreElements()) {
                    sb.append(", ");
                }
            }
            if(log.isErrorEnabled()) log.error(ExternalStrings.Discovery_THE_FOLLOWING_PROPERTIES_ARE_NOT_RECOGNIZED__0, sb);
            return false;
        }
        return true;
    }

    @Override // GemStoneAddition
    public void resetStats() {
        super.resetStats();
        num_discovery_requests=0;
    }

    @Override // GemStoneAddition
    public void start() throws Exception {
        super.start();
        PingSender ping_sender=new PingSender(timeout, num_ping_requests, this);
        if(ping_waiter == null)
            ping_waiter=new PingWaiter(timeout, num_initial_members, this, ping_sender);
    }

    /** GemStoneAddition - tell the waiter it can start its countdown now */
    public final void wakeWaiter(AtomicBoolean waiter_sync) {
        synchronized(waiter_sync) {
          waiter_sync.set(true);
          waiter_sync.notifyAll();
        }
    }

    @Override // GemStoneAddition
    public void stop() {
        is_server=false;
        if(ping_waiter != null)
            ping_waiter.stop();
    }

    /**
     * Finds the initial membership
     * @return Vector of PingRsp
     */
    public Vector findInitialMembers() {
        return ping_waiter != null? ping_waiter.findInitialMembers() : null;
    }

    public String findInitialMembersAsString() {
        Vector results=findInitialMembers();
        if(results == null || results.size() == 0) return "<empty>";
        PingRsp rsp;
        StringBuffer sb=new StringBuffer();
        for(Iterator it=results.iterator(); it.hasNext();) {
            rsp=(PingRsp)it.next();
            sb.append(rsp).append("\n");
        }
        return sb.toString();
    }


    /**
     * An event was received from the layer below. Usually the current layer will want to examine
     * the event type and - depending on its type - perform some computation
     * (e.g. removing headers from a MSG event type, or updating the internal membership list
     * when receiving a VIEW_CHANGE event).
     * Finally the event is either a) discarded, or b) an event is sent down
     * the stack using <code>PassDown</code> or c) the event (or another event) is sent up
     * the stack using <code>PassUp</code>.
     * <p/>
     * For the PING protocol, the Up operation does the following things.
     * 1. If the event is a Event.MSG then PING will inspect the message header.
     * If the header is null, PING simply passes up the event
     * If the header is PingHeader.GET_MBRS_REQ then the PING protocol
     * will PassDown a PingRequest message
     * If the header is PingHeader.GET_MBRS_RSP we will add the message to the initial members
     * vector and wake up any waiting threads.
     * 2. If the event is Event.SET_LOCAL_ADDR we will simple set the local address of this protocol
     * 3. For all other messages we simple pass it up to the protocol above
     *
     * @param evt - the event that has been sent from the layer below
     */

    @Override // GemStoneAddition
    public void up(Event evt) {
        Message msg, rsp_msg;
//        Object obj; GemStoneAddition
        PingHeader hdr, rsp_hdr;
        PingRsp rsp;
        Address coord = null;

        switch(evt.getType()) {

        case Event.MSG:
            msg=(Message)evt.getArg();
//            obj=msg.getHeader(getName());  GemStoneAddition - do null check after removeHeader instead
//            if(obj == null || !(obj instanceof PingHeader)) {
//                passUp(evt);
//                return;
//            }
            hdr=(PingHeader)msg.removeHeader(getName());
            if (hdr == null) {
              passUp(evt);
              return;
            }

            switch(hdr.type) {

            case PingHeader.GET_MBRS_REQ:   // return Rsp(local_addr, coord)
                if(local_addr != null && msg.getSrc() != null && local_addr.equals(msg.getSrc())) {
                    if(trace)
                        log.trace("discarded my own discovery request");
                    return;
                }
                // GemStoneAddition - don't respond if not done with initial discovery and we're
                // using a gossip server
//                if (getName().equals("TCPGOSSIP") && !is_server) {  deadcoded for Fraser release - caused concurrent startup split-brain in locators 
//                  return;
//                }
                synchronized(members) {
//                coord=members.size() > 0 ? (Address)members.firstElement() : local_addr;
                  // GemStoneAddition - if no membership set, get a likely
                  // candidate from the ping waiter.  if that fails, use the
                  // local address
                  if (members.size() > 0) {
                    // GemStoneAddition - split brain detection
                    for (int i=0; i<members.size(); i++) {
                      Address mbr = (Address)members.get(i);
                      if (mbr.preferredForCoordinator()) {
                        coord = mbr;
                        break;
                      }
                    }
                  }
                  else {
                    coord = this.ping_waiter.getPossibleCoordinator(local_addr);
                    if (coord == null) {
                      if (local_addr.preferredForCoordinator()) {  // GemStoneAddition - split brain detection
                        coord = local_addr;
                      }
                    }
                  }
                  // GemStoneAddition
                  // when floating coordinators are disabled, the new member
                  // might be a restarted locator that should become the new
                  // coordinator
                  if (coord == null && msg.getSrc().preferredForCoordinator()) {
                    coord = msg.getSrc();
                  }
                }

                if (trace) {// GemStoneAddition - debugging membership
                  if (coord != null) {
                    log.trace("using " + (coord.equals(local_addr) ? "myself" : coord.toString())
                      + " as coordinator response to GET_MBRS_REQ");
                  }
                  else {
                    log.trace("unable to determine a potential coordinator in response to GET_MBRS_REQ");
                  }
                }
                
                // GemStoneAddition - if no potential coordinators, don't respond
                if (coord == null) {
                  break;
                }

                // GemStoneAddition - if we're still in discovery in this VM,
                // then use the request as a ping response (bug 30341)
                // update: actually, this just causes the VM to find another young
                // VM and increases the likelyhood of falling into the concurrent_startup
                // algorithm
                //if (coord == local_addr && ping_waiter != null) {
                //  ping_waiter.addResponse(new PingRsp(msg.getSrc(), msg.getSrc(), false));
                //}
                    
                PingRsp ping_rsp=new PingRsp(local_addr, coord, is_server);
                rsp_msg=new Message(msg.getSrc(), null, null);
                rsp_hdr=new PingHeader(PingHeader.GET_MBRS_RSP, ping_rsp);
                rsp_msg.putHeader(getName(), rsp_hdr);
                if(trace)
                    log.trace("received GET_MBRS_REQ from " + msg.getSrc() + ", sending response " + rsp_hdr);
                passDown(new Event(Event.MSG, rsp_msg));
                return;

            case PingHeader.GET_MBRS_RSP:   // add response to vector and notify waiting thread
                rsp=hdr.arg;

                if(trace)
                    log.trace("received GET_MBRS_RSP, rsp=" + rsp);
                ping_waiter.addResponse(rsp);
                return;

            default:
                if(warn) { 
                  log.warn( 
                    ExternalStrings.
                    Discovery_GOT_PING_HEADER_WITH_UNKNOWN_TYPE_0
                    .toLocalizedString(new Byte(hdr.type)));
                }
                return;
            }
            break;


        case Event.SET_LOCAL_ADDRESS:
            passUp(evt);
            local_addr=(Address)evt.getArg();
            localAddressSet(local_addr);
            break;

        case Event.CONNECT_OK:
            handleConnectOK();
            passUp(evt);
            break;

        default:
            passUp(evt);            // Pass up to the layer above us
            break;
        }
    }


    /**
     * An event is to be sent down the stack. The layer may want to examine its type and perform
     * some action on it, depending on the event's type. If the event is a message MSG, then
     * the layer may need to add a header to it (or do nothing at all) before sending it down
     * the stack using <code>PassDown</code>. In case of a GET_ADDRESS event (which tries to
     * retrieve the stack's address from one of the bottom layers), the layer may need to send
     * a new response event back up the stack using <code>passUp()</code>.
     * The PING protocol is interested in several different down events,
     * Event.FIND_INITIAL_MBRS - sent by the GMS layer and expecting a GET_MBRS_OK
     * Event.TMP_VIEW and Event.VIEW_CHANGE - a view change event
     * Event.BECOME_SERVER - called after client has joined and is fully working group member
     * Event.CONNECT, Event.DISCONNECT.
     */
    @Override // GemStoneAddition
    public void down(Event evt) {

        switch(evt.getType()) {

        case Event.FIND_INITIAL_MBRS:   // sent by GMS layer, pass up a GET_MBRS_OK event
            // sends the GET_MBRS_REQ to all members, waits 'timeout' ms or until 'num_initial_members' have been retrieved
            num_discovery_requests++;
            //if (num_discovery_requests == 1) {
            //  log.getLogWriter().info("DEBUG: bypassing findInitialMembers to force retry");
            //  passUp(new Event(Event.FIND_INITIAL_MBRS_OK, new java.util.Vector(0)));
            //}
            //else {
              if (ping_waiter != null) // GemStoneAddition - delayed creation of waiter
                ping_waiter.start();
            //}
            break;

        case Event.TMP_VIEW:
        case Event.VIEW_CHANGE:
            Vector tmp;
            if((tmp=((View)evt.getArg()).getMembers()) != null) {
                synchronized(members) {
                    members.clear();
                    members.addAll(tmp);
                }
            }
            passDown(evt);
            break;

        case Event.BECOME_SERVER: // called after client has joined and is fully working group member
            passDown(evt);
            is_server=true;
            break;

        case Event.CONNECT:
            group_addr=(String)evt.getArg();
            passDown(evt);
            handleConnect();
            break;

        case Event.DISCONNECT:
            handleDisconnect();
            passDown(evt);
            break;

        default:
            passDown(evt);          // Pass on to the layer below us
            break;
        }
    }



    /** GemStoneAddition - tell other processes that I am the coordinator */
//    protected void sendIAMCoordinator() {
//      if (members.size() > 0) {
//        if (members.get(0).equals(local_addr)) {
//          PingRsp ping_rsp=new PingRsp(local_addr, local_addr, true);
//          Message rsp_msg=new Message(null, null, null);
//          Header rsp_hdr=new PingHeader(PingHeader.GET_MBRS_RSP, ping_rsp);
//          rsp_msg.putHeader(getName(), rsp_hdr);
//          rsp_msg.bundleable = false; // GemStoneAddition
//          if(trace)
//              log.trace("multicasting decision to become coordinator");
//          passDown(new Event(Event.MSG, rsp_msg));
//        }
//      }
//    }

    /* -------------------------- Private methods ---------------------------- */


    protected View makeView(Vector mbrs) {
        Address coord;
        long id;
        ViewId view_id=new ViewId(local_addr);

        coord=view_id.getCoordAddress();
        id=view_id.getId();

        return new View(coord, id, mbrs);
    }



}
