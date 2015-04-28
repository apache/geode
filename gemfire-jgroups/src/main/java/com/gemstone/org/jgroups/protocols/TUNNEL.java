/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: TUNNEL.java,v 1.20 2005/12/08 09:35:28 belaban Exp $


package com.gemstone.org.jgroups.protocols;



import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.stack.RouterStub;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Util;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;
import java.util.Vector;




/**
 * Replacement for UDP. Instead of sending packets via UDP, a TCP connection is opened to a Router
 * (using the RouterStub client-side stub),
 * the IP address/port of which was given using channel properties <code>router_host</code> and
 * <code>router_port</code>. All outgoing traffic is sent via this TCP socket to the Router which
 * distributes it to all connected TUNNELs in this group. Incoming traffic received from Router will
 * simply be passed up the stack.
 * <p>A TUNNEL layer can be used to penetrate a firewall, most firewalls allow creating TCP connections
 * to the outside world, however, they do not permit outside hosts to initiate a TCP connection to a host
 * inside the firewall. Therefore, the connection created by the inside host is reused by Router to
 * send traffic from an outside host to a host inside the firewall.
 * @author Bela Ban
 */
public class TUNNEL extends Protocol implements Runnable {
    final Properties properties=null;
    String channel_name=null;
    final Vector members=new Vector();
    String router_host=null;
    int router_port=0;
    Address local_addr=null;  // sock's local addr and local port
    Thread receiver=null; // GemStoneAddition synchronize on this to access
    RouterStub stub=null;
    private final Object stub_mutex=new Object();

    /** If true, messages sent to self are treated specially: unicast messages are
     * looped back immediately, multicast messages get a local copy first and -
     * when the real copy arrives - it will be discarded. Useful for Window
     * media (non)sense */
    boolean  loopback=true;

    private final Reconnector reconnector=new Reconnector();
    private final Object reconnector_mutex=new Object();

    /** If set it will be added to <tt>local_addr</tt>. Used to implement
     * for example transport independent addresses */
    byte[]          additional_data=null;

    /** time to wait in ms between reconnect attempts */
    long            reconnect_interval=5000;


    public TUNNEL() {
    }


    @Override // GemStoneAddition  
    public String toString() {
        return "Protocol TUNNEL(local_addr=" + local_addr + ')';
    }




    /*------------------------------ Protocol interface ------------------------------ */

    @Override // GemStoneAddition  
    public String getName() {
        return "TUNNEL";
    }

    @Override // GemStoneAddition  
    public void init() throws Exception {
        super.init();
    }

    @Override // GemStoneAddition  
    public void start() throws Exception {
        createTunnel();  // will generate and pass up a SET_LOCAL_ADDRESS event
        passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
    }

    @Override // GemStoneAddition  
    public void stop() {
      synchronized (this) { // GemStoneAddition
        if(receiver != null) {
          receiver.interrupt(); // GemStoneAddition
            receiver=null;
        }
      }
        teardownTunnel();
        stopReconnector();
    }



    /**
     * This prevents the up-handler thread to be created, which essentially is superfluous:
     * messages are received from the network rather than from a layer below.
     * DON'T REMOVE ! 
     */
    @Override // GemStoneAddition  
    public void startUpHandler() {
    }

    /** Setup the Protocol instance acording to the configuration string */
    @Override // GemStoneAddition  
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("router_host");
        if(str != null) {
            router_host=str;
            props.remove("router_host");
        }

        str=props.getProperty("router_port");
        if(str != null) {
            router_port=Integer.parseInt(str);
            props.remove("router_port");
        }

        if(log.isDebugEnabled()) {
            log.debug("router_host=" + router_host + ";router_port=" + router_port);
        }

        if(router_host == null || router_port == 0) {
            if(log.isErrorEnabled()) {
                log.error(ExternalStrings.TUNNEL_BOTH_ROUTER_HOST_AND_ROUTER_PORT_HAVE_TO_BE_SET_);
                return false;
            }
        }

        str=props.getProperty("reconnect_interval");
        if(str != null) {
            reconnect_interval=Long.parseLong(str);
            props.remove("reconnect_interval");
        }

        str=props.getProperty("loopback");
        if(str != null) {
            loopback=Boolean.valueOf(str).booleanValue();
            props.remove("loopback");
        }

        if(props.size() > 0) {
            StringBuffer sb=new StringBuffer();
            for(Enumeration e=props.propertyNames(); e.hasMoreElements();) {
                sb.append(e.nextElement().toString());
                if(e.hasMoreElements()) {
                    sb.append(", ");
                }
            }
            if(log.isErrorEnabled()) log.error(ExternalStrings.TUNNEL_THE_FOLLOWING_PROPERTIES_ARE_NOT_RECOGNIZED__0, sb);
            return false;
        }
        return true;
    }


    /** Caller by the layer above this layer. We just pass it on to the router. */
    @Override // GemStoneAddition  
    public void down(Event evt) {
        Message      msg;
        TunnelHeader hdr;
        Address dest;

        if(evt.getType() != Event.MSG) {
            handleDownEvent(evt);
            return;
        }

        hdr=new TunnelHeader(channel_name);
        msg=(Message)evt.getArg();
        dest=msg.getDest();
        msg.putHeader(getName(), hdr);

        if(msg.getSrc() == null)
            msg.setSrc(local_addr);

        if(trace)
            log.trace(msg + ", hdrs: " + msg.getHeaders());

        // Don't send if destination is local address. Instead, switch dst and src and put in up_queue.
        // If multicast message, loopback a copy directly to us (but still multicast). Once we receive this,
        // we will discard our own multicast message
        if(loopback && (dest == null || dest.equals(local_addr) || dest.isMulticastAddress())) {
            Message copy=msg.copy();
            // copy.removeHeader(name); // we don't remove the header
            copy.setSrc(local_addr);
            // copy.setDest(dest);
            evt=new Event(Event.MSG, copy);

            /* Because Protocol.up() is never called by this bottommost layer, we call up() directly in the observer.
               This allows e.g. PerfObserver to get the time of reception of a message */
            if(observer != null)
                observer.up(evt, up_queue.size());
            if(trace) log.trace("looped back local message " + copy);
            passUp(evt);
            if(dest != null && !dest.isMulticastAddress())
                return;
        }



        if(!stub.isConnected()) {
            startReconnector();
        }
        else {
            if(stub.send(msg, channel_name) == false) {
                startReconnector();
            }
        }
    }


    /** Creates a TCP connection to the router */
    void createTunnel() throws Exception {
        if(router_host == null || router_port == 0)
            throw new Exception("router_host and/or router_port not set correctly; tunnel cannot be created");

        synchronized(stub_mutex) {
            stub=new RouterStub(router_host, router_port);
            local_addr=stub.connect();
            if(additional_data != null && local_addr instanceof IpAddress)
                ((IpAddress)local_addr).setAdditionalData(additional_data);
        }
        if(local_addr == null)
            throw new Exception("could not obtain local address !");
    }


    /** Tears the TCP connection to the router down */
    void teardownTunnel() {
        synchronized(stub_mutex) {
            if(stub != null) {
                stub.disconnect();
                stub=null;
            }
        }
    }

    /*--------------------------- End of Protocol interface -------------------------- */






    public void run() {
        Message msg;

        if(stub == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.TUNNEL_ROUTER_STUB_IS_NULL_CANNOT_RECEIVE_MESSAGES_FROM_ROUTER_);
            return;
        }

        for (;;) { // GemStoneAddition remove coding anti-pattern
          if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
            msg=stub.receive();
            if(msg == null) {
              if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition -- for safety
//                if(receiver == null) break; GemStoneAddition
                if(log.isTraceEnabled()) log.trace("received a null message. Trying to reconnect to router");
                if(!stub.isConnected())
                    startReconnector();
                try { // GemStoneAddition
                Util.sleep(5000);
                }
                catch (InterruptedException e) {
                  break; // exit loop and thread
                }
                continue;
            }
            handleIncomingMessage(msg);
        }
    }





    /* ------------------------------ Private methods -------------------------------- */




    public void handleIncomingMessage(Message msg) {
        TunnelHeader hdr=(TunnelHeader)msg.removeHeader(getName());

        // discard my own multicast loopback copy
        if(loopback) {
            Address dst=msg.getDest();
            Address src=msg.getSrc();

            if(dst != null && dst.isMulticastAddress() && src != null && local_addr.equals(src)) {
                if(trace)
                    log.trace("discarded own loopback multicast packet");
                return;
            }
        }

         if(trace)
             log.trace(msg + ", hdrs: " + msg.getHeaders());

        /* Discard all messages destined for a channel with a different name */

        String ch_name=hdr != null? hdr.channel_name : null;
        if(ch_name != null && !channel_name.equals(ch_name))
            return;

        passUp(new Event(Event.MSG, msg));
    }


    void handleDownEvent(Event evt) {
        if(trace)
            log.trace(evt);

        switch(evt.getType()) {

        case Event.TMP_VIEW:
        case Event.VIEW_CHANGE:
            synchronized(members) {
                members.removeAllElements();
                Vector tmpvec=((View)evt.getArg()).getMembers();
                for(int i=0; i < tmpvec.size(); i++)
                    members.addElement(tmpvec.elementAt(i));
            }
            break;

        case Event.GET_LOCAL_ADDRESS:   // return local address -> Event(SET_LOCAL_ADDRESS, local)
            passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
            break;

        case Event.SET_LOCAL_ADDRESS:
            local_addr=(Address)evt.getArg();
            if(local_addr instanceof IpAddress && additional_data != null)
                ((IpAddress)local_addr).setAdditionalData(additional_data);
            break;

        case Event.CONNECT:
            channel_name=(String)evt.getArg();
            if(stub == null) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.TUNNEL_CONNECT__ROUTER_STUB_IS_NULL);
            }
            else {
                stub.register(channel_name);
            }

            synchronized (this) { // GemStoneAddition
            receiver=new Thread(this, "TUNNEL receiver thread");
            }
            receiver.setDaemon(true);
            receiver.start();

            passUp(new Event(Event.CONNECT_OK));
            break;

        case Event.DISCONNECT:
            if(receiver != null) {
                receiver=null;
                if(stub != null)
                    stub.disconnect();
            }
            teardownTunnel();
            passUp(new Event(Event.DISCONNECT_OK));
            passUp(new Event(Event.SET_LOCAL_ADDRESS, null));
            break;

        case Event.CONFIG:
            if(log.isDebugEnabled()) log.debug("received CONFIG event: " + evt.getArg());
            handleConfigEvent((HashMap)evt.getArg());
            break;
        }
    }

    private void startReconnector() {
        synchronized(reconnector_mutex) {
            reconnector.start();
        }
    }

    private void stopReconnector() {
        synchronized(reconnector_mutex) {
            reconnector.stop();
        }
    }

    void handleConfigEvent(HashMap map) {
        if(map == null) return;
        if(map.containsKey("additional_data"))
            additional_data=(byte[])map.get("additional_data");
    }

    /* ------------------------------------------------------------------------------- */


    protected/*GemStoneAddition*/ class Reconnector implements Runnable {
        Thread  my_thread=null; // GemStoneAddition - access synchronized on this


        public void start() {
            synchronized(this) {
                if(my_thread == null || !my_thread.isAlive()) {
                    my_thread=new Thread(this, "Reconnector");
                    my_thread.setDaemon(true);
                    my_thread.start();
                }
            }
        }

        public void stop() {
            synchronized(this) {
              if (my_thread != null) my_thread.interrupt(); // GemStoneAddition
                my_thread=null;
            }
        }


        public void run() {
            for (;;) { // GemStoneAddition - remove coding anti-pattern
              if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
                if(stub.reconnect()) {
                    stub.register(channel_name);
                    if(log.isDebugEnabled()) log.debug("reconnected");
                    return;
                }
                try { // GemStoneAddition
                  Util.sleep(reconnect_interval);
                }
                catch (InterruptedException e) {
                  break; // exit loop and thread
                }
            }
        }
    }


}
