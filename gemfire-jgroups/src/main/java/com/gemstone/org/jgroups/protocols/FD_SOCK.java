/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 *  
 *  This class has been extensively modified for GemFire to get rid of numerous
 *  startup/shutdown race conditions and concurrent programming anti-patterns.
 *  Methods were added to allow other sources of suspicion to initiate checks
 *  and SUSPECT events.  A random health check task was also added that runs
 *  in the background and selects a random member to check up on, using a new
 *  PROBE_TERMINATION to terminate the connection.
 *  
 *  PROBE_TERMINATION is also
 *  used in all cases where a normal disconnect is done and NORMAL_TERMINATION
 *  now means that the JGroups stack is disconnecting.  Other members treat
 *  this as a LEAVE event, so we have TCP/IP confirmation that a member is
 *  leaving and do not need to rely solely on GMS UDP datagram messages to
 *  know that this is happening.
 **/
// $Id: FD_SOCK.java,v 1.30 2005/10/19 12:12:56 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;

import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Global;
import com.gemstone.org.jgroups.Header;
import com.gemstone.org.jgroups.JChannel;
import com.gemstone.org.jgroups.JGroupsVersion;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.SuspectMember;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.ViewId;
import com.gemstone.org.jgroups.protocols.VERIFY_SUSPECT.VerifyHeader;
import com.gemstone.org.jgroups.stack.BoundedLinkedHashMap;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.BoundedList;
import com.gemstone.org.jgroups.util.ConnectionWatcher;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.Promise;
import com.gemstone.org.jgroups.util.SockCreator;
import com.gemstone.org.jgroups.util.Streamable;
import com.gemstone.org.jgroups.util.TimeScheduler;
import com.gemstone.org.jgroups.util.Util;

/**
 * Failure detection protocol based on sockets. Failure detection is ring-based. Each member creates a
 * server socket and announces its address together with the server socket's address in a multicast. A
 * pinger thread will be started when the membership goes above 1 and will be stopped when it drops below
 * 2. The pinger thread connects to its neighbor on the right and waits until the socket is closed. When
 * the socket is closed by the monitored peer in an abnormal fashion (IOException), the neighbor will be
 * suspected.<p> The main feature of this protocol is that no ping messages need to be exchanged between
 * any 2 peers, and failure detection relies entirely on TCP sockets. The advantage is that no activity
 * will take place between 2 peers as long as they are alive (i.e. have their server sockets open).
 * The disadvantage is that hung servers or crashed routers will not cause sockets to be closed, therefore
 * they won't be detected.
 * The FD_SOCK protocol will work for groups where members are on different hosts<p>
 * The costs involved are 2 additional threads: one that
 * monitors the client side of the socket connection (to monitor a peer) and another one that manages the
 * server socket. However, those threads will be idle as long as both peers are running.
 * @author Bela Ban May 29 2001
 */
public class FD_SOCK extends Protocol implements Runnable {
    long                get_cache_timeout=3000;            // msecs to wait for the socket cache from the coordinator
    static/*GemStoneAddition*/ final long          get_cache_retry_timeout=500;       // msecs to wait until we retry getting the cache from coord
    long                suspect_msg_interval=5000;         // (BroadcastTask): mcast SUSPECT every 5000 msecs
    long                lastPingSelectionSuspectProcessingTime = 0L; // GemStoneAddition - for limiting suspect processing we've added in determinePingDest
    int                 num_tries=20;                       // attempts coord is solicited for socket cache until we give up
    final Vector        members=new Vector(50);            // list of group members (updated on VIEW_CHANGE)
    final List          randomMembers = new ArrayList(50);
    volatile View       currentView; // GemStoneAddition
    boolean             srv_sock_sent=false;               // has own socket been broadcast yet ?
    final Vector        pingable_mbrs=new Vector(11);      // mbrs from which we select ping_dest. may be subset of 'members'
    final Promise       get_cache_promise=new Promise();   // used for rendezvous on GET_CACHE and GET_CACHE_RSP
    boolean             got_cache_from_coord=false;        // was cache already fetched ?
    Address             local_addr=null;                   // our own address
    volatile/*GemStoneAddition*/ ServerSocket        srv_sock=null;                     // server socket to which another member connects to monitor me
    InetAddress         srv_sock_bind_addr=null;           // the NIC on which the ServerSocket should listen
    volatile/*GemStoneAddition*/ ServerSocketHandler srv_sock_handler=null;             // accepts new connections on srv_sock
    volatile/*GemStoneAddition*/ IpAddress           srv_sock_addr=null;                // pair of server_socket:port
    volatile /*GemStoneAddition*/ Address             ping_dest=null;                    // address of the member we monitor
    volatile Socket              ping_sock=null;                    // socket to the member we monitor // GemStoneAddition - volatile
    InputStream         ping_input=null;                   // input stream of the socket to the member we monitor
    
    // GemStoneAddition: updates to pinger_thread synchronized on this
    volatile Thread     pinger_thread=null;                // listens on ping_sock, suspects member if socket is closed
    final Hashtable     cache=new Hashtable(11);           // keys=Addresses, vals=IpAddresses (socket:port)
    
    int connectTimeout = 5000; // GemStoneAddition - timeout attempts to connect

    /** Start port for server socket (uses first available port starting at start_port). A value of 0 (default)
     * picks a random port */
    int                 start_port=1; // GemStoneAddition - start at 1
    int end_port=65535; // GemStoneAddition
    Random ran = new Random(); // GemStoneAddition
    final Map<Address, Promise> ping_addr_promises = new HashMap();
//    final Promise       ping_addr_promise=new Promise();   // to fetch the ping_addr for ping_dest
    final Object        sock_mutex=new Object();           // for access to ping_sock, ping_input
    boolean             socket_closed_in_mutex; // GemStoneAddition - fix race between unsuspect & connecting to stale ping_dest
    TimeScheduler       timer=null;
    final BroadcastTask bcast_task=new BroadcastTask();    // to transmit SUSPECT message (until view change)
    volatile boolean    regular_sock_close=false;         // used by interruptPingerThread() when new ping_dest is computed.  GemStoneAddition - volatile
    int                 num_suspect_events=0;
    private static final int NORMAL_TERMINATION=9;
    private static final int PROBE_TERMINATION=6; // GemStoneAddition
    private static final int ABNORMAL_TERMINATION=-1;
    private static final String name="FD_SOCK";
    
    // GemStoneAddition - result status from setupPingSocket
    private static final int SETUP_OK = 0;
    private static final int SETUP_FAILED = 1;
    private static final int SETUP_RESELECT = 2; // reselect ping_dest

    BoundedList          suspect_history=new BoundedList(20);
    
    // GemStoneAddition: test hook - how many other members have sent NORMAL_TERMINATION status?
    public int normalDisconnectCount;
    // GemStoneAddition: test hook - is FD_SOCK connected to the member it's supposed to be watching?
    public volatile boolean isConnectedToPingDest;
    
    @Override // GemStoneAddition  
    public String getName() {
        return name;
    }

    // start GemStoneAddition
    @Override // GemStoneAddition  
    public int getProtocolEnum() {
      return com.gemstone.org.jgroups.stack.Protocol.enumFD;
    }
    // end GemStone addition

    public String getLocalAddress() {return local_addr != null? local_addr.toString() : "null";}
    public String getMembers() {return members != null? members.toString() : "null";}
    public String getPingableMembers() {return pingable_mbrs != null? pingable_mbrs.toString() : "null";}
    public String getPingDest() {return ping_dest != null? ping_dest.toString() : "null";}
    public int getNumSuspectEventsGenerated() {return num_suspect_events;}
    public String printSuspectHistory() {
        StringBuffer sb=new StringBuffer();
        for(Enumeration en=suspect_history.elements(); en.hasMoreElements();) {
            sb.append(new Date()).append(": ").append(en.nextElement()).append("\n");
        }
        return sb.toString();
    }

    @Override // GemStoneAddition  
    public boolean setProperties(Properties props) {
        String str, tmp=null;

        super.setProperties(props);
        str=props.getProperty("get_cache_timeout");
        if(str != null) {
            get_cache_timeout=Long.parseLong(str);
            props.remove("get_cache_timeout");
        }

        str=props.getProperty("suspect_msg_interval");
        if(str != null) {
            suspect_msg_interval=Long.parseLong(str);
            props.remove("suspect_msg_interval");
        }

        str=props.getProperty("num_tries");
        if(str != null) {
            num_tries=Integer.parseInt(str);
            props.remove("num_tries");
        }

        str=props.getProperty("start_port");
        if(str != null) {
            start_port=Integer.parseInt(str);
            props.remove("start_port");
        }
        
        // GemStoneAddition - end port
        str=props.getProperty("end_port");
        if(str != null) {
            end_port=Integer.parseInt(str);
            props.remove("end_port");
        }
        
        // PropertyPermission not granted if running in an untrusted environment with JNLP.
        try {
            tmp=System.getProperty("bind.address");
            if(Util.isBindAddressPropertyIgnored()) {
                tmp=null;
            }
        }
        catch (SecurityException ex){
        }

        if(tmp != null)
            str=tmp;
        else
            str=props.getProperty("srv_sock_bind_addr");
        
        // GemStoneAddition - make compatible with other bind-address selection
        if (str == null) {
          str = System.getProperty("gemfire.jg-bind-address");
          if (str != null) {
            if (str.length() == 0) {
              str = null;
            }
          }
        }
        if(str != null) {
            try {
                srv_sock_bind_addr=InetAddress.getByName(str);
            }
            catch(UnknownHostException unknown) {
                if(log.isFatalEnabled()) log.fatal("(srv_sock_bind_addr): host " + str + " not known");
                return false;
            }
            props.remove("srv_sock_bind_addr");
        }
        else {
          try {
            srv_sock_bind_addr=stack.gfBasicFunctions.getLocalHost();
          }
          catch (UnknownHostException e) {
            // ignore
          }
        }
        
        str=props.getProperty("connect_timeout");
        if (str != null) {
          this.connectTimeout = Integer.parseInt(str);
          props.remove("connect_timeout");
        }


        if(props.size() > 0) {
            log.error(ExternalStrings.FD_SOCK_FD_SOCKSETPROPERTIES_THE_FOLLOWING_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);
            return false;
        }
        return true;
    }


    @Override // GemStoneAddition  
    public void init() throws Exception {
        normalDisconnectCount = 0;
        srv_sock_handler=new ServerSocketHandler();
        timer=stack != null ? stack.timer : null;
        if(timer == null)
            throw new Exception("FD_SOCK.init(): timer == null");
    }

    /**
     * Ensures that the static class ClientConnectionHandler
     * is loaded.
     * 
     * @see SystemFailure#loadEmergencyClasses()
     */
    // GemStoneAddition
    public static void loadEmergencyClasses() {
      ClientConnectionHandler.loadEmergencyClasses();
    }
    
    /**
     * Kill the pinger and server threads and their underlying sockets
     * 
     * @see SystemFailure#emergencyClose()
     * @see #stop()
     */
    public void emergencyClose() { // GemStoneAddition
//      System.err.println("FD_SOCK#emergencyClose");
      //  stop();
      //  ...which is: stopServerSocket(); stopPingerThread();

      ServerSocketHandler ssh = srv_sock_handler; // volatile fetch
      if (ssh != null) {
        ssh.emergencyClose();
      }
      
      // stopPingerThread();
      Thread thr = pinger_thread;
      if (thr != null) {
        thr.interrupt();
      }
      Socket sock = ping_sock;
      if (sock != null) {
        try {
          sock.close();
        }
        catch (IOException e) {
          // ignore
        }
      }
    }

    @Override // GemStoneAddition  
    public void stop() {
        bcast_task.removeAll();
        Thread t = pinger_thread; // GemStoneAddition, use this for a termination
        pinger_thread = null;
        if(t != null && t.isAlive()) {
            regular_sock_close=true; // (set this before interrupting the pinger)
            t.interrupt(); // GemStoneAddition
            sendPingTermination(true); // GemStoneAddition
            teardownPingSocket();
        }
        // GemStoneAddition - do not perform normal termination if we're being kicked out of the system
        Exception e = ((JChannel)stack.getChannel()).getCloseException();
        boolean normalShutdown = (e == null);
//        log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "failure detection is shutting down with normal="+normalShutdown, e);
        stopServerSocket(normalShutdown);
    }

    @Override // GemStoneAddition  
    public void resetStats() {
        super.resetStats();
        num_suspect_events=0;
        suspect_history.removeAll();
    }

    private boolean isCoordinator; // GemStoneAddition
    private volatile boolean disconnecting; // GemStoneAddition - don't suspect others while disconnecting
    private BoundedList sockNotFound = new BoundedList(100); // GemStoneAddition - initiate suspect processing if no socket for a mbr
    
    @Override // GemStoneAddition  
    public void up(Event evt) {
        Message msg;
        final FdHeader hdr;

        switch(evt.getType()) {

        case Event.SET_LOCAL_ADDRESS:
            local_addr=(Address) evt.getArg();
            break;

        case Event.CONNECT_OK: // GemStoneAddition - be proactive about the cache for bug #41772
          passUp(evt);
          sendIHaveSockMessage(null, local_addr, srv_sock_addr);  // unicast message to msg.getSrc()
          if (!got_cache_from_coord) { // GemStoneAddition - get the cache now
            getCacheFromCoordinator();
            got_cache_from_coord=true;
          }
          // bug #50752 - when Jgroups is used for all communication in GemFire
          // the random ping task can be too aggressive if there are problems
          // with the FD_SOCK address cache
          if ((this.stack != null && this.stack.gfPeerFunctions != null)
              && !this.stack.gfPeerFunctions.getDisableTcp()
              && !Boolean.getBoolean("gemfire.disable-random-health-checks")) {
            timer.add(new RandomPingTask());
          }
          break;

        case Event.MSG:
            msg=(Message) evt.getArg();
            hdr=(FdHeader) msg.removeHeader(name);
            if(hdr == null)
                break;  // message did not originate from FD_SOCK layer, just pass up

            switch(hdr.type) {

            case FdHeader.SUSPECT:
              if (hdr.mbrs.contains(this.local_addr)) {
                if (!srv_sock_handler.beSick) {
                  // GemStoneAddition - if someone's suspecting this member send them a ping
                  Message rsp=new Message(msg.getSrc(), local_addr, null);
                  rsp.putHeader(VERIFY_SUSPECT.name, new VerifyHeader(VerifyHeader.I_AM_NOT_DEAD, local_addr));
                  passDown(new Event(Event.MSG, rsp));
                  hdr.mbrs.remove(this.local_addr);
                  if (hdr.mbrs.size() == 0) {
                    return;  // no need to pass this event up the stack
                  }
                }
              }
              
              // GemStoneAddition - bug 42146: lots of suspect messages logged during disconnect
              if (this.disconnecting || this.stack.getChannel().closing()) {
                break;
              }
              // GemStoneAddition - if the sender isn't in this member's view,
              // and this is the coordinator, he may have been ousted from
              // the system and should be told so
              View cur = currentView;
              if (hdr.vid != null && cur != null
                  && hdr.vid.compareTo(cur.getVid()) < 0 // fix for bug 39142
                  && !isInMembership(msg.getSrc())) {
                Message notMbr = new Message();
                log.getLogWriter().info(
                    ExternalStrings.FD_SOCK_RECEIVED_SUSPECT_NOTIFICATION_FROM_NONMEMBER_0_USING_VIEW_ID_1_SENDING_NOT_MEMBER_RESPONSE,
                    new Object[] { msg.getSrc(), hdr.vid});
                Header fhdr = new FdHeader(FdHeader.NOT_MEMBER);
                notMbr.putHeader(this.getName(), fhdr);
                notMbr.setDest(msg.getSrc());
                passDown(new Event(Event.MSG, notMbr));
                break;
              }
              // GemStoneAddition - log the notification
              String reason = "";
              ReasonHeader rhdr = (ReasonHeader)msg.getHeader("REASON");
              if (rhdr != null) {
                reason = " Reason="+rhdr.cause;
              }
              // GemStoneAddition - if there are any actual members that are
              // newly suspect, then announce them.  Otherwise be quiet
              boolean anyMembers = false;
              for (int i=0; i<hdr.mbrs.size(); i++) {
                if (this.members.contains(hdr.mbrs.elementAt(i)) && !bcast_task.isSuspectedMember((Address)hdr.mbrs.elementAt(i)) ) {
                  anyMembers = true;
                  break;
                }
              }
              
              if (anyMembers) {
                log.getLogWriter().info(
                  ExternalStrings.FD_RECEIVED_SUSPECT_NOTIFICATION_FOR_MEMBERS_0_FROM_1_2,
                  new Object[] {hdr.mbrs, msg.getSrc(), reason});
              }

//              if(hdr.mbrs != null) GemStoneAddition (cannot be null) 
              {
                    if(log.isDebugEnabled()) log.debug("[SUSPECT] hdr=" + hdr);
                    for(int i=0; i < hdr.mbrs.size(); i++) {
                        Address mbr = (Address)hdr.mbrs.elementAt(i); // GemStoneAddition - get it once instead of twice
                        passUp(new Event(Event.SUSPECT, new SuspectMember(msg.getSrc(), mbr))); // GemStoneAddition SuspectMember
                        passDown(new Event(Event.SUSPECT, new SuspectMember(msg.getSrc(), mbr)));
                        if (!this.members.contains(mbr) && ((IpAddress)this.local_addr).splitBrainEnabled()) {
                          passUp(new Event(Event.SUSPECT_NOT_MEMBER, mbr));
                        }
                    }
                }
//                else GemStoneAddition (cannot be null)
//                    if(warn) log.warn("[SUSPECT]: hdr.mbrs == null");
                break;

            case FdHeader.FD_SUSPECT: // GemStoneAddition - FD suspects a member so try to connect to it
              Address suspectMbr = hdr.mbr;
              checkSuspect(suspectMbr, null);
              return;  // that's the end for this message

              // If I have the sock for 'hdr.mbr', return it. Otherwise look it up in my cache and return it
            case FdHeader.WHO_HAS_SOCK:
                if(local_addr != null && local_addr.equals(msg.getSrc()))
                    return; // don't reply to WHO_HAS bcasts sent by me !

                if(hdr.mbr == null) {
                    if(log.isErrorEnabled()) log.error(ExternalStrings.FD_SOCK_HDRMBR_IS_NULL);
                    return;
                }

                if(trace) log.trace("who-has-sock " + hdr.mbr);

                // 1. Try my own address, maybe it's me whose socket is wanted
                if(local_addr != null && local_addr.equals(hdr.mbr) && srv_sock_addr != null) {
                    sendIHaveSockMessage(msg.getSrc(), local_addr, srv_sock_addr);  // unicast message to msg.getSrc()
                    return;
                }

                // 2. If I don't have it, maybe it is in the cache
                if(cache.containsKey(hdr.mbr))
                    sendIHaveSockMessage(msg.getSrc(), hdr.mbr, (IpAddress) cache.get(hdr.mbr));  // ucast msg
                break;


                // Update the cache with the addr:sock_addr entry (if on the same host)
            case FdHeader.I_HAVE_SOCK:
                if(hdr.mbr == null || hdr.sock_addr == null) {
                    if(log.isErrorEnabled()) log.error(ExternalStrings.FD_SOCK_I_HAVE_SOCK_HDRMBR_IS_NULL_OR_HDRSOCK_ADDR__NULL);
                    return;
                }

                // if(!cache.containsKey(hdr.mbr))
                cache.put(hdr.mbr, hdr.sock_addr); // update the cache
                // GemStoneAddition - sockNotFound
                synchronized(sockNotFound) {
                  sockNotFound.removeElement(hdr.mbr);
                }
                if(trace) log.trace("i-have-sock: " + hdr.mbr + " --> " +
                                                   hdr.sock_addr + " (cache is " + cache + ')');
                // GemStoneAddition - promises are kept in a map so we can fetch the address
                // of a member that is not the ping_dest
                Promise ping_addr_promise;
                synchronized(ping_addr_promises) {
                  ping_addr_promise = ping_addr_promises.get(hdr.mbr);
                }
//                if(ping_dest != null && hdr.mbr.equals(ping_dest))
                if (ping_addr_promise != null) {
                    ping_addr_promise.setResult(hdr.sock_addr);
                }
                break;

                // Return the cache to the sender of this message
            case FdHeader.GET_CACHE:
                if(hdr.mbr == null) {
                    if(log.isErrorEnabled()) log.error(ExternalStrings.FD_SOCK_GET_CACHE_HDRMBR__NULL);
                    return;
                }
                FdHeader rsphdr=new FdHeader(FdHeader.GET_CACHE_RSP); // GemStoneAddition - fix for bug #43839
                rsphdr.cachedAddrs=(Hashtable) cache.clone();
                if (trace) {
                  log.trace("FD_SOCK: sending cache to " + hdr.mbr);
                }
                msg=new Message(hdr.mbr, null, null);
                msg.putHeader(name, rsphdr);
                msg.isHighPriority = true; // GemStoneAddition
                passDown(new Event(Event.MSG, msg));
                break;

            case FdHeader.GET_CACHE_RSP:
                if(hdr.cachedAddrs == null) {
                    if(log.isErrorEnabled()) log.error(ExternalStrings.FD_SOCK_GET_CACHE_RSP_CACHE_IS_NULL);
                    return;
                }
                // GemStoneAddition - this code was moved from getCacheFromCoordinator() so that
                // we can install the cache even if the get-cache-timeout expires
                cache.putAll(hdr.cachedAddrs); // replace all entries (there should be none !) in cache with the new values
                if(trace) log.trace("got cache from " + msg.getSrc() + ": cache is " + cache);
                // GemStoneAddition - sockNotFound
                synchronized(sockNotFound) {
                  for (Iterator it=hdr.cachedAddrs.keySet().iterator(); it.hasNext(); ) {
                    Object key = it.next();
                    sockNotFound.removeElement(key);
                  }
                }
                get_cache_promise.setResult(hdr.cachedAddrs);
                break;


                // GemStoneAddition - we transmitted a suspect message to a coordinator
            // who doesn't know about us.
            case FdHeader.NOT_MEMBER:
              // respond by ousting this process from membership
              passUp(new Event(Event.EXIT, stack.gfBasicFunctions.getForcedDisconnectException(
                ExternalStrings.FD_SOCK_THIS_MEMBER_HAS_BEEN_FORCED_OUT_OF_THE_DISTRIBUTED_SYSTEM_BECAUSE_IT_DID_NOT_RESPOND_WITHIN_MEMBERTIMEOUT_MILLISECONDS_FD_SOCK.toLocalizedString())));
              break;
            }
            // unhandled FdHeader type
            return;
        }

        passUp(evt);                                        // pass up to the layer above us
    }

//    public void passDown(Event evt) {
//      try {
//        if (evt.getType() == Event.MSG){
//          Message msg = (Message)evt.getArg();
//          FdHeader header = msg.getHeader(name);
//          if (header != null && header.type == FdHeader.SUSPECT) {
//            log.getLogWriterI18n().info(JGroupsStrings.DEBUG,
//                "passing down suspect message: " + msg, new Exception("passDown stack trace"));
//          }
//        }
//      } finally {
//        super.passDown(evt);
//      }
//    }

    @Override // GemStoneAddition  
    public void down(Event evt) {
        Address mbr, tmp_ping_dest;
        View v;

        switch(evt.getType()) {

            case Event.UNSUSPECT:
              mbr = (Address)evt.getArg();
              log.getLogWriter().info(ExternalStrings.
                  FD_SOCK_FAILURE_DETECTION_RECEIVED_NOTIFICATION_THAT_0_IS_NO_LONGER_SUSPECT,
                  mbr); 
              synchronized(this) { // GemStoneAddition - same lock order as VIEW_CHANGE 
                // GemStoneAddition - fix for bug 39114, hang waiting for view removing dead member
                synchronized(pingable_mbrs) {
                  if (bcast_task.isSuspectedMember(mbr)) {
                    bcast_task.removeSuspectedMember((Address)evt.getArg());
                    Thread thr = pinger_thread; // GemStoneAddition: volatile fetch
                    if (thr != null && thr.isAlive()) {
                      interruptPingerThread(); // allows the thread to use the new socket
                      startPingerThread();
                    }
                    else {
                      startPingerThread(); // only starts if not yet running
                    }
                  }
                }
                synchronized(sockNotFound) {
                  sockNotFound.removeElement(mbr);
                }
              }
              passDown(evt);  // GemStoneAddition - pass down to FD
              break;

            case Event.GMS_SUSPECT:  // GemStoneAddition - view ack failure handling
              if (this.stack.getChannel().closing()) break;
              java.util.List gmsSuspects = (java.util.List)evt.getArg();
              for (Iterator it=gmsSuspects.iterator(); it.hasNext(); ) {
                Address suspectMbr = (Address)it.next();
                checkSuspect(suspectMbr, "Did not respond to a view change");
              }
              return;

            case Event.CONNECT:
              // GemStoneAddition - connect the socket now so that we can send an I_HAVE_SOCK
              // message in CONNECT_OK processing
                this.disconnecting = false;
                srv_sock=Util.createServerSocket(srv_sock_bind_addr, start_port,
                    end_port);
                srv_sock_addr=new IpAddress(srv_sock_bind_addr, srv_sock.getLocalPort());
                passDown(evt);
                startServerSocket();
                //if(pinger_thread == null)
                  //  startPingerThread();
                break;
                
            case Event.CONNECT_OK:
              passDown(evt); // GemStoneAddition - pass to other protocols (tho they don't currently use it)
              break;

            // GemStoneAddition - stop sending suspect messages at once
            case Event.DISCONNECTING:
              this.disconnecting = true;
              passDown(evt);
              break;

            case Event.VIEW_CHANGE:
                passDown(evt); // GemStoneAddition - pass down outside of sync
                synchronized(this) {
                    v=(View) evt.getArg();
                    synchronized(members) {
                      currentView = v; // GemStoneAddition - track the current view id
                      members.removeAllElements();
                      members.addAll(v.getMembers());
                    }
                    synchronized(randomMembers) {
                      randomMembers.clear();
                      randomMembers.addAll(v.getMembers());
                      Collections.shuffle(randomMembers);
                    }
                    bcast_task.adjustSuspectedMembers(members);
                    synchronized(pingable_mbrs) {
                      Address coord = v.getCreator(); // GemStoneAddition
                      this.isCoordinator = this.local_addr != null
                        && coord != null
                        && this.local_addr.equals(coord);
                      pingable_mbrs.removeAllElements();
                      pingable_mbrs.addAll(members);
                    }

                    //if(log.isDebugEnabled()) log.debug("VIEW_CHANGE received: " + members);

                    // 3. Remove all entries in 'cache' which are not in the new membership
                    for(Enumeration e=cache.keys(); e.hasMoreElements();) {
                        mbr=(Address) e.nextElement();
                        if(!members.contains(mbr))
                            cache.remove(mbr);
                    }

                    if(members.size() > 1) {
                      synchronized (this) {
                        Thread thr = pinger_thread; // GemStoneAddition: volatile fetch
                        if(thr != null && thr.isAlive()) {
                          tmp_ping_dest=determinePingDest();
                          if(ping_dest != null && tmp_ping_dest != null 
                              && !ping_dest.equals(tmp_ping_dest)) {
                              interruptPingerThread(); // allows the thread to use the new socket
                              startPingerThread();
                          }
                      }
                      else
                          startPingerThread(); // only starts if not yet running
                      }
                    }
                    else {
                        ping_dest=null;
                        interruptPingerThread();
                    }
                }
//                log.getLogWriter().info("failure detection is finished processing a view");
                break;

            default:
                passDown(evt);
                break;
        }
    }
    
    /**
     * GemStoneAddition if this is the coordinator, see if the member is in the
     * current view.  Otherwise punt and say he is in the view
     */
    private boolean isInMembership(Address sender) {
      if (pingable_mbrs != null) {
        synchronized(pingable_mbrs) {
          if (this.isCoordinator) {
            Set m = new HashSet(pingable_mbrs);
            return m.contains(sender);
          }
        }
      }
      return true;
    }
        


    /**
     * Runs as long as there are 2 members and more. Determines the member to be monitored and fetches its
     * server socket address (if n/a, sends a message to obtain it). The creates a client socket and listens on
     * it until the connection breaks. If it breaks, emits a SUSPECT message. It the connection is closed regularly,
     * nothing happens. In both cases, a new member to be monitored will be chosen and monitoring continues (unless
     * there are fewer than 2 members).
     */
    public void run() {
        Address cached_ping_dest;
        IpAddress ping_addr;
        Address log_addr = null;

        // GemStoneAddition - we need to pay attention to the member-timeout here and
        // not wait for too long.  Initiate suspect processing if we can't get the socket
        // in that amount of time
        int max_fetch_tries=this.connectTimeout * 2 / 4000;  // number of times a socket address is to be requested before giving up
        if (max_fetch_tries < 1) {
          max_fetch_tries = 1;
        }
        int max_fetch_tries_reset = max_fetch_tries;

        if(trace) log.trace("pinger_thread started"); // +++ remove

        // 1. Get the addr:pid cache from the coordinator (only if not already fetched)
        if(!got_cache_from_coord) {
            getCacheFromCoordinator();
            got_cache_from_coord=true;
        }

        // 2. Broadcast my own addr:sock to all members so they can update their cache
        if(!srv_sock_sent) {
            if(srv_sock_addr != null) {
                sendIHaveSockMessage(null, // send to all members
                        local_addr,
                        srv_sock_addr);
                srv_sock_sent=true;
            }
        }

        for(;;) { // GemStoneAddition remove coding anti-pattern
          if (Thread.currentThread().isInterrupted()) {  // GemStoneAddition -- for safety
            if (log.isDebugEnabled()) {
              log.info("FD_SOCK interrupted - pinger thread exiting");
            }
            break;
          }
          synchronized (this) {  // GemStoneAddition -- look for termination
            if (pinger_thread == null) {
              if (log.isDebugEnabled()) {
                log.info("FD_SOCK pinger_thread is null - pinger thread exiting");
              }
              break;
            }
          }
          
          // Recalculate peer every time through the loop...
            synchronized(pingable_mbrs) { // GemStoneAddition - setting of ping_dest must be protected
              ping_dest=cached_ping_dest=determinePingDest();
              if(log.isDebugEnabled()) {
                log.debug("determinePingDest()=" + cached_ping_dest + ", pingable_mbrs=" + pingable_mbrs
                    +", suspects="+bcast_task.suspected_mbrs);
              }
              // GemStoneAddition - log when ping_dest changes
              if (log_addr != ping_dest && ping_dest != null) {
                log_addr = ping_dest;
                log.getLogWriter().info(ExternalStrings.FD_SOCK_WATCHING_0, log_addr);
              }
              
            }
            if(cached_ping_dest == null) {
                if (log.isInfoEnabled()) {
                  log.info("FD_SOCK unable to determine new ping_dest - pinger thread exiting");
                }
//                ping_dest=null; GemStoneAddition redundant nulling of variable
//                pinger_thread=null; GemStoneAddition do this at bottom of method
                break;
            }
            ping_addr=fetchPingAddress(cached_ping_dest);
            if(ping_addr == null) {
                if (log.isTraceEnabled() && // GemStoneAddition
                    log.isErrorEnabled()) log.error(ExternalStrings.FD_SOCK_SOCKET_ADDRESS_FOR__0__COULD_NOT_BE_FETCHED_RETRYING, cached_ping_dest);
                max_fetch_tries--;  // GemStoneAddition - was a pre-decrement
                if(max_fetch_tries <= 0) {
                  // GemStoneAddition - suspect the ping_dest and go on to the
                  // next member - fix for bug #41772
                  suspect(cached_ping_dest, false, "Unable to determine failure detection socket for this member");
                  max_fetch_tries = max_fetch_tries_reset;
                  continue;
//                    if (log.isWarnEnabled()) {
//                      log.warn("Unable to find FD_SOCK address for " + ping_dest);
//                    }
//                    break;
                }
                try { // GemStoneAddition
                  Util.sleep(1000);
                }
                catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  if (log.isInfoEnabled()) {
                    log.info("FD_SOCK interrupted - pinger thread exiting");
                  }
                  break; // treat as though attempts have failed
                }
                continue;
            }
            
            if (Thread.currentThread().isInterrupted()) {
              break;
            }

            switch (setupPingSocket(ping_addr, cached_ping_dest)) { // GemStoneAddition added reselect logic
              case SETUP_FAILED:
//                boolean shutdown = this.stack.jgmm.isShuttingDown((IpAddress)tmp_ping_dest);
                if (log.isDebugEnabled()) {
                  log.trace("ping setup failed.  member " + cached_ping_dest /*+ " shutdown status = " + shutdown*/);
                }
//                if (shutdown) {
//                  // if the member is shutting down it's already in LEAVE state in the coordinator
//                  continue;
//                }
                // covers use cases #7 and #8 in GmsTests.txt
                if (!stack.getChannel().closing()/* && !stack.jgmm.isShuttingDown((IpAddress)tmp_ping_dest)*/) {
                  if(log.isDebugEnabled()) log.debug("could not create socket to " + ping_dest + "; suspecting " + ping_dest);
                  //broadcastSuspectMessage(ping_dest);
                  suspect(cached_ping_dest, false, ExternalStrings.FD_SOCK_COULD_NOT_SET_UP_PING_SOCKET.toLocalizedString()); // GemStoneAddition
                  continue;
                } else {
                  break;
                }
              case SETUP_RESELECT:
                continue;
              case SETUP_OK:
                isConnectedToPingDest = true; // GemStoneAddition - test hook
                break;
            }
            
            if (stack.getChannel().closing()) {
              break;
            }

            if(log.isDebugEnabled()) log.debug("ping_dest=" + cached_ping_dest 
                + ", ping_sock=" + ping_sock + ", cache=" + cache);

            // at this point ping_input must be non-null, otherwise setupPingSocket() would have thrown an exception
            try {
                if(ping_input != null) {
                    int c=ping_input.read();
                    switch(c) {
                        case NORMAL_TERMINATION:
                          normalDisconnectCount++; // GemStoneAddition - test hook
                            if(log.isDebugEnabled())
                                log.debug("peer closed socket normally");
                            // bug 38268 - pinger thread does not exit on interruptPingerThread under jrockit 1.5
                            // and pinger thread is never restarted after normal departure of ping_dest
                            //synchronized(this) { // GemStoneAddition - synch
                            //  pinger_thread=null;
                            //}
                            bcast_task.addDepartedMember(cached_ping_dest);
                            log.getLogWriter().info(ExternalStrings.DEBUG, "Member " + cached_ping_dest + " shut down with normal termination.");
                            passUp(new Event(Event.FD_SOCK_MEMBER_LEFT_NORMALLY, cached_ping_dest));
                            teardownPingSocket();
                            break;
                        case PROBE_TERMINATION: // GemStoneAddition
                          if (log.isDebugEnabled()) {
                            log.debug("peer closed socket with probe notification");
                          }
                          teardownPingSocket();
                          normalDisconnectCount++;
                          ping_sock.getOutputStream().flush();
                          ping_sock.shutdownOutput();
                          break;
                        case ABNORMAL_TERMINATION:
                            handleSocketClose(null);
                            break;
                        default:
                            break;
                    }
                }
            }
            catch(IOException ex) {  // we got here when the peer closed the socket --> suspect peer and then continue
                handleSocketClose(ex);
            }
            catch (VirtualMachineError err) { // GemStoneAddition
              // If this ever returns, rethrow the error.  We're poisoned
              // now, so don't let this thread continue.
              throw err;
            }
            catch(Throwable catch_all_the_rest) {
              log.error(ExternalStrings.FD_SOCK_EXCEPTION, catch_all_the_rest);
            }
            finally {
              isConnectedToPingDest = false; // GemStoneAddition - test hook
            }
        } // for
        if(log.isDebugEnabled()) log.debug("pinger thread terminated");
        synchronized(this) { // GemStoneAddition - synch
          pinger_thread=null;
        }
    }


    /* ----------------------------------- test methods ----------------------------------------- */
    
    /**
     * clears the Address -> FD port cache
     */
    public void clearAddressCache() {
      this.cache.clear();
    }


    /* ----------------------------------- Private Methods -------------------------------------- */
    
    /**
     * GemStoneAddition - GemFire suspects a member
     * @return true if the member is okay
     */
    public boolean checkSuspect(Address suspectMbr, String reason) {
      if (bcast_task.isDepartedMember(suspectMbr) /*|| stack.jgmm.isShuttingDown((IpAddress)mbr)*/) {
        return true; // no need to suspect a member that just left 
      }
      if (bcast_task.isSuspectedMember(suspectMbr)) {
        return false;
      }
      IpAddress sockAddr = fetchPingAddress(suspectMbr);
      boolean isFdSockServer = true;
      if (sockAddr == null) {
        boolean notFound;
        synchronized(sockNotFound) {
          notFound = sockNotFound.contains(suspectMbr);
        }
        if (notFound) {
          if (!this.disconnecting && !this.stack.getChannel().closing()) {
            suspect(suspectMbr, false, ExternalStrings.FD_SOCK_UNABLE_TO_CHECK_STATE_OF_UNRESPONSIVE_MEMBER_0.toLocalizedString(suspectMbr));
          }
        } else {
          log.getLogWriter().warning(ExternalStrings.FD_SOCK_UNABLE_TO_CHECK_STATE_OF_UNRESPONSIVE_MEMBER_0, suspectMbr);
          synchronized(sockNotFound) {
            sockNotFound.add(suspectMbr);
          }
        }
        return true;
      }
      return checkSuspect(suspectMbr, sockAddr, reason, isFdSockServer, true);
    }
    
    /**
     * GemStoneAddition - FD suspects a member so try to connect
     * to it.  If we can't connect to it, suspect the mbr
     * @param isFdSockServer is the address an FD_SOCK address?
     * @param initiateSuspectProcessing if suspect processing should be initiated here
     * @return true if the member is okay
     */
    public boolean checkSuspect(Address mbr, IpAddress dest, String reason, boolean isFdSockServer, boolean initiateSuspectProcessing) {
      if (this.disconnecting || this.stack.getChannel().closing()) {
        // do not suspect others while disconnecting since they may see
        // the view change w/o this member before disconnect completes
        return true;
      }
      if (bcast_task.isDepartedMember(mbr) /*|| stack.jgmm.isShuttingDown((IpAddress)mbr)*/) {
        return true; // no need to suspect a member that just left 
      }
      if (bcast_task.isSuspectedMember(mbr)) {
        return false;
      }
      if (log.isDebugEnabled()) {
        log.debug("Attempting to connect to member " + mbr + " at address " + dest + " reason: " + reason);
      }
      Socket sock = null;
      boolean connectSucceeded = false;
      boolean noProbeResponse = false;
      try {
        // bug #44930 - socket.connect(timeout) does not honor timeout
        // bug #45097 - NIO socket will fail to connect with IPv6 address on Windows
        SockCreator sc = JChannel.getGfFunctions().getSockCreator();
        boolean useSSL = !isFdSockServer && sc.useSSL();
        sock = sc.connect(dest.getIpAddress(), dest.getPort(),
            connectTimeout, new ConnectTimerTask(), false, -1, useSSL);
        if (sock == null) {
          if (log.isDebugEnabled()) {
            log.debug("Attempt to connect to " + mbr + " at address " + dest + " failed.  connect() returned null");
          }
          connectSucceeded = false;
        } else {
          if (log.isDebugEnabled()) {
            log.debug("Attempt to connect to " + mbr + " at address " + dest + " succeeded.");
          }
          connectSucceeded = true;
          noProbeResponse = false;
          if (isFdSockServer) {
            if (log.isDebugEnabled()) {
              log.debug("Attempt to read probe response from " + mbr);
            }
            sock.setSoLinger(true, connectTimeout);
            //okay, we connected to it, so now we know the other member's
            //FD_SOCK is okay (at least) and some other part of the system
            //is just being jittery
            sock.getOutputStream().write(PROBE_TERMINATION);
            sock.getOutputStream().flush();
            sock.shutdownOutput();
            sock.setSoTimeout(connectTimeout); // read the PROBE_TERMINATION
            try {
              int response = sock.getInputStream().read();
              if (log.isDebugEnabled()) {
                log.debug("Attempt to read probe response from " + mbr + " returned " + response );
              }
              noProbeResponse = (response != PROBE_TERMINATION);
            } catch (SocketTimeoutException e) {
              noProbeResponse = true;
            }
            if (noProbeResponse) {
              // the member's socket is still open and accepting connections but the member
              // is not responding to the probe. Allow suspect processing to take place
              // if it's running a version of the product that ought to have responded
              noProbeResponse = false;
              if (mbr.getVersionOrdinal() >= JGroupsVersion.GFE_81_ORDINAL) {
                noProbeResponse = true;
              }
            }
          }
          sock.close();
        }
      }
      catch (IOException e) {
        // expected
      }
      catch (IllegalStateException e) {
        // expected - bug #44469
      }
      finally {
        if (sock != null && !sock.isClosed()) {
          try {
            sock.close();
          }
          catch (IOException ioe) {
            // sheesh
          }
        }
        if ((!connectSucceeded || noProbeResponse) && initiateSuspectProcessing) {
          // GemStoneAddition - suspected members are tracked by broadcaster, as in FD
          suspect(mbr, false, reason==null?ExternalStrings.FD_SOCK_UNABLE_TO_CONNECT_TO_THIS_MEMBER.toLocalizedString():reason);
        }
      }
      if (log.isDebugEnabled()) {
        if (isFdSockServer && noProbeResponse) {
          log.debug("FD_SOCK found that " + mbr
              + " is accepting connections on " + dest.getIpAddress() + ":" + dest.getPort()
              + " but did not return a probe response");
        } else {
          log.debug("FD_SOCK found that " + mbr + (connectSucceeded? " is " : " is not ")
              + "accepting connections on " + dest.getIpAddress() + ":" + dest.getPort());
        }
      }
      if (isFdSockServer && noProbeResponse) {
        return false;
      } else {
        return connectSucceeded;
      }
    }


    void handleSocketClose(Exception ex) {
        teardownPingSocket();     // make sure we have no leftovers
        if(!regular_sock_close) { // only suspect if socket was not closed regularly (by interruptPingerThread())
            if(log.isDebugEnabled())
                log.debug("peer " + ping_dest + " closed socket (" + (ex != null ? ex.getClass().getName() : "eof") + ')');
            suspect(ping_dest, true, "Socket was not closed nicely"); // GemStoneAddition
            //broadcastSuspectMessage(ping_dest);
        }
        else {
            if(log.isDebugEnabled()) log.debug("socket to " + ping_dest + " was reset");
            regular_sock_close=false;
        }
    }


    synchronized void startPingerThread() { // GemStoneAddition - synchronization
        Thread t = pinger_thread; // GemStoneAddition, get the reference once
        if(t == null || !t.isAlive()) {
            t=new Thread(GemFireTracer.GROUP, this, "FD_SOCK Ping thread");
            t.setDaemon(true);
            t.start();
            pinger_thread = t;
        }
        
    }


//    /**
//     * Similar to interruptPingerThread, this method should only be used
//     * when JGroups is stopping.
//     */
//    synchronized void stopPingerThread() { // GemStoneAddition - synchronization
////        pinger_thread=null; GemStoneAddition set flag before whacking the thread
         ////// GEMSTONE: CODE HAS BEEN IN-LINED IN stop()
//    }
    
    // GemStoneAddition - send something so the connection handler
    // can exit
    synchronized void sendPingTermination(boolean stopping) {
      Socket ps = ping_sock;
      if (ps != null && !ps.isClosed()) {
        try {
          if (log.isDebugEnabled()) {
            log.trace("sending normal FD_SOCK ping termination");
          }
          // GemStoneAddition - when shutting down we send a NORMAL_TERMINATION
          // which tells the other member that we're shutting down.  Otherwise
          // we send a PROBE_TERMINATION, which is more akin to how JGroups
          // FD_SOCK termination originally worked.  This lets the other member
          // know not to start suspecting this member during shutdown.
          if (stopping) {
            ps.getOutputStream().write(NORMAL_TERMINATION);
          } else {
            ps.getOutputStream().write(PROBE_TERMINATION);
          }
          ps.getOutputStream().flush();
        }
        catch (java.io.IOException ie) {
          if (trace)
            log.trace("FD_SOCK io exception sending ping termination", ie);
        }
      }
    }


    /**
     * Interrupts the pinger thread. The Thread.interrupt() method doesn't seem to work under Linux with JDK 1.3.1
     * (JDK 1.2.2 had no problems here), therefore we close the socket (setSoLinger has to be set !) if we are
     * running under Linux. This should be tested under Windows. (Solaris 8 and JDK 1.3.1 definitely works).<p>
     * Oct 29 2001 (bela): completely removed Thread.interrupt(), but used socket close on all OSs. This makes this
     * code portable and we don't have to check for OSs.
     * <p>
     * see com.gemstone.org.jgroups.tests.InterruptTest to determine whether Thread.interrupt() works for InputStream.read().
     */
    synchronized void interruptPingerThread() { // GemStoneAddition - synchronization
        Thread pt = pinger_thread; // GemStoneAddition volatile read
        if(pt != null && pt.isAlive()) {
            regular_sock_close=true;
            sendPingTermination(false); // GemStoneAddition
            teardownPingSocket(); // will wake up the pinger thread. less elegant than Thread.interrupt(), but does the job
            if (log.isDebugEnabled()) {
              log.debug("'Interrupted' pinger thread");
            }
      }
    }

    void startServerSocket() {
        if(srv_sock_handler != null)
            srv_sock_handler.start(); // won't start if already running
    }

    void stopServerSocket(boolean normalTermination) {
        if(srv_sock_handler != null)
            srv_sock_handler.stop(normalTermination);
    }


    /**
     * Creates a socket to <code>dest</code>, and assigns it to ping_sock. Also assigns ping_input
     */
    int setupPingSocket(IpAddress dest, Address mbr) {
        synchronized(sock_mutex) {
          if (socket_closed_in_mutex) {
            // GemStoneAddition - another thread closed the ping socket
            socket_closed_in_mutex = false;
            return SETUP_RESELECT;
          }
            if(dest == null) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.FD_SOCK_DESTINATION_ADDRESS_IS_NULL);
                return SETUP_RESELECT;
            }
            try {
                // GemStoneAddition - set a shorter wait than the default
                //ping_sock=new Socket(dest.getIpAddress(), dest.getPort());
//                log.getLogWriter().info("DEBUG: failure detection is attempting to connect to " + mbr);
                ping_sock = new Socket();
                java.net.InetSocketAddress address =
                  new java.net.InetSocketAddress(dest.getIpAddress(), dest.getPort());
                ping_sock.connect(address, connectTimeout);
//                log.getLogWriter().info("DEBUG: failure detection has connected to " + mbr);
                // end GemStoneAddition

                ping_sock.setSoLinger(true, connectTimeout);
                ping_input=ping_sock.getInputStream();
                return SETUP_OK;
            }
            catch (VirtualMachineError err) { // GemStoneAddition
              // If this ever returns, rethrow the error.  We're poisoned
              // now, so don't let this thread continue.
              throw err;
            }
            catch(Throwable ex) {
                return SETUP_FAILED;
            }
        }
    }


    synchronized void teardownPingSocket() { // GemStoneAddition - synch
      synchronized(sock_mutex) {
        if (ping_sock != null) {
          // GemStoneAddition - if the other member's machine crashed, we
          // may hang trying to close the socket. That causes bad things to
          // happen if this is a UDP receiver thread.  So, close the socket in
          // another thread.
          final Socket old_ping_sock = ping_sock;
          final InputStream old_ping_input = ping_input;
          (new Thread(GemFireTracer.GROUP, "GemFire FD_SOCK Ping Socket Teardown Thread") {
            @Override // GemStoneAddition  
            public void run() {
//            if(old_ping_sock != null) {
                try {
                  socket_closed_in_mutex = true;
                    old_ping_sock.shutdownInput();
                    old_ping_sock.close();
                }
                catch(Exception ex) {
                }
//                ping_sock=null;
//            }
            if(old_ping_input != null) {
                try {
                    old_ping_input.close();
                }
                catch(Exception ex) {
                }
//                ping_input=null;
            }
            }
          }).start();
//          ping_sock = null;
//          ping_input = null;
        }
      }
    }


    /**
     * GemStoneAddition - this method has been changed to spawn a thread to obtain
     * the cache.  There is no reason to stall startup while waiting for the FD_SOCK
     * cache to arrive.  Processing of the cache has been moved to up() when the
     * cache is received.
     * 
     * Determines coordinator C. If C is null and we are the first member, return. Else loop: send GET_CACHE message
     * to coordinator and wait for GET_CACHE_RSP response. Loop until valid response has been received.
     */
    void getCacheFromCoordinator() {
      Thread cacheThread = new Thread(GemFireTracer.GROUP, "FD_SOCK cache initialization") {
        @Override
        public void run() {
        Address coord;
        int attempts=num_tries;
        Message msg;
        FdHeader hdr;
        Hashtable result = null; // GemStoneAddition - initialize the variable

        get_cache_promise.reset();
        while(attempts > 0) {
            if((coord=determineCoordinator()) != null) {
                if(coord.equals(local_addr)) { // we are the first member --> empty cache
                  if (members.size() > 1) {
                    coord = (Address)members.elementAt(1);
                  } else {
                    if(log.isDebugEnabled()) log.debug("first member; cache is empty");
                    return;
                  }
                }
                if (trace)
                  log.trace("FD_SOCK requesting cache from " + coord);
                hdr=new FdHeader(FdHeader.GET_CACHE);
                hdr.mbr=local_addr;
                msg=new Message(coord, null, null);
                msg.putHeader(name, hdr);
                msg.isHighPriority = true; // GemStoneAddition
                passDown(new Event(Event.MSG, msg));
                result=(Hashtable) get_cache_promise.getResult(get_cache_timeout);
                if(result != null) {
                  // GemStoneAddition - processing of the cache is done on receipt, not here
                    return;
                }
                else {
                    //if(log.isErrorEnabled()) log.error("received null cache; retrying");  // GemStoneAddition - commented out
                }
            }

            try { // GemStoneAddition
              Util.sleep(get_cache_retry_timeout);
            }
            catch (InterruptedException e) {
              // Treat as though no more attempts
              Thread.currentThread().interrupt();
              break;
            }
            --attempts;
        }
        }};
        cacheThread.setDaemon(true);
        cacheThread.start();
    }
    
    // GemStoneAddition - is the system disconnecting?
    public boolean isDisconnecting() {
      return stack.gfPeerFunctions.isDisconnecting();
    }


    /**
     * Sends a SUSPECT message to all group members. Only the coordinator (or the next member in line if the coord
     * itself is suspected) will react to this message by installing a new view. To overcome the unreliability
     * of the SUSPECT message (it may be lost because we are not above any retransmission layer), the following scheme
     * is used: after sending the SUSPECT message, it is also added to the broadcast task, which will periodically
     * re-send the SUSPECT until a view is received in which the suspected process is not a member anymore. The reason is
     * that - at one point - either the coordinator or another participant taking over for a crashed coordinator, will
     * react to the SUSPECT message and issue a new view, at which point the broadcast task stops.
     */
    public void suspect(Address suspected_mbr // GemStoneAddition - public
          ,boolean abnormalTermination ,String reason) { // GemStoneAddition
//        Message suspect_msg;
        FdHeader hdr;

        if(suspected_mbr == null) return;
        
        if (FD_SOCK.this.disconnecting || stack.getChannel().closing()) {
          return;
        }
        
        if (suspected_mbr.equals(local_addr)) {
          //log.getLogWriter().severe("suspecting myself!", new Exception("stack trace"));
          return; // GemStoneAddition - don't suspect self
        }
        
        // GemStoneAddition - if the member is already suspect, the broadcast
        // task will send periodic suspect msgs until it is unsuspected or
        // booted from membership
        if (bcast_task.isSuspectedMember(suspected_mbr)) { 
          if (log.getLogWriter().fineEnabled()) {
            log.getLogWriter().fine("not resuspecting suspected member " + suspected_mbr);
          }
          return;
        }

        // GemStoneAddition - if it's not in the view anymore don't send suspect messages
        synchronized(members) {
          if (suspected_mbr.getBirthViewId() < currentView.getVid().getId()
              && !members.contains(suspected_mbr)) {
            if (log.getLogWriter().fineEnabled()) {
              log.getLogWriter().fine("not suspecting departed member " + suspected_mbr);
            }
            return;
          }
        }
        
        if (bcast_task.isDepartedMember(suspected_mbr)) {
          // it's already gone - don't suspect it anymore
//          if(trace) log.trace("not suspecting departed member " + suspected_mbr);
          if (log.getLogWriter().fineEnabled()) {
            log.getLogWriter().fine("not suspecting departed member " + suspected_mbr);
          }
          return;
        }
        
        /* before suspecting another process make sure I can connect to myself */
        if (!checkSuspect(this.local_addr, "checking that this process can connect to itself")) {
          // bug #43796 - FD_SOCK can be aggressive about other members if the VM runs out of file descriptors
          return;
        }
        
//        if(trace) log.trace("suspecting " + suspected_mbr + " (own address is " + local_addr + "): " + reason);
        log.getLogWriter().info(ExternalStrings.DEBUG, "suspecting member " + suspected_mbr);

        // 1. Send a SUSPECT message right away; the broadcast task will take some time to send it (sleeps first)
        hdr=new FdHeader(FdHeader.SUSPECT);
        hdr.abnormalTermination = abnormalTermination; // GemStoneAddition - suspect acceleration
        hdr.mbrs=new Vector(1);
        hdr.mbrs.addElement(suspected_mbr);
        View cur = currentView; // volatile read
        if (cur == null) { // GemStoneAddition - too soon to suspect anyone - haven't received a view!
          return;
        }
        hdr.vid = cur.getVid(); // GemStoneAddition - view id in header
        sendSuspectMessage(hdr, reason);

        // 2. Add to broadcast task and start latter (if not yet running). The task will end when
        //    suspected members are removed from the membership
        bcast_task.addSuspectedMember(suspected_mbr);
        if(stats) {
            num_suspect_events++;
            suspect_history.add(suspected_mbr);
        }
    }
    
    void sendSuspectMessage(FdHeader hdr, String reason) {
      Message suspect_msg;
      View cur = currentView; // volatile read
      ReasonHeader rhdr = new ReasonHeader(reason);
      
      if (isDisconnecting()) {
        return;
      }
      suspect_msg=new Message();
      suspect_msg.putHeader(name, hdr);
      suspect_msg.putHeader("REASON", rhdr); // GemStoneAddition
      if (cur.getMembers().size() < 5) {
        if (trace)
          log.getLogWriter().info(ExternalStrings.DEBUG, "sending suspect message with view " + cur);
        passDown(new Event(Event.MSG, suspect_msg));
      } else {
        // GemStoneAddition - only send suspect msg to myself, the locators and one random member
        if (trace)
          log.getLogWriter().info(ExternalStrings.DEBUG, "sending suspect message to eligible coordinators and one random member");
        int count = 0;
        Vector<Address> mbrs = cur.getMembers();
        Vector<Address> notSentTo = new Vector(mbrs.size());
        for (Address target: mbrs) {
          boolean isMe = target.equals(this.local_addr);
          if (isMe || target.preferredForCoordinator()) {
            suspect_msg.setDest(target);
//            log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "ucasting suspect message to " + target);
            passDown(new Event(Event.MSG, suspect_msg));
            count += 1;
            suspect_msg=new Message();
            suspect_msg.putHeader(name, hdr);
            suspect_msg.putHeader("REASON", rhdr); // GemStoneAddition
          }
          else {
            notSentTo.add(target);
          }
        }
        // GemStoneAddition - now choose a random member to send it to
        if (notSentTo.size() > 0) {
          int mbrIdx = ran.nextInt(notSentTo.size());
          Address mbr = notSentTo.elementAt(mbrIdx);
          suspect_msg.setDest(mbr);
//          log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "ucasting suspect message to " + mbr);
          passDown(new Event(Event.MSG, suspect_msg));
        }
      }
    }


  /**
   * [GemStoneAddition] a <code>Header</code> that has a reason, so we
   * can see what caused a suspected member event
   */
  public static class ReasonHeader extends Header implements Streamable {
            protected String cause;

    /**
     * Constructor for <code>Externalizable</code>
     */
    public ReasonHeader() {

    }

    ReasonHeader(String reason) {
      cause = reason;
    }

    @Override // GemStoneAddition  
    public long size(short version) {
      return cause.getBytes().length;
    }

    @Override // GemStoneAddition  
    public String toString() {
      return cause;
    }

    public void writeExternal(ObjectOutput out)
      throws IOException {
      out.writeUTF(cause);
    }

    public void readExternal(ObjectInput in)
      throws IOException, ClassNotFoundException {
      cause = in.readUTF();
    }

    public void writeTo(DataOutputStream out)
      throws IOException {
      out.writeUTF(cause);
    }

    public void readFrom(DataInputStream in)
      throws IOException, IllegalAccessException, InstantiationException {
      cause = in.readUTF();
    }

  }
    void broadcastWhoHasSockMessage(Address mbr) {
        Message msg;
        FdHeader hdr;

        if(local_addr != null && mbr != null)
            if(log.isDebugEnabled()) log.debug("[" + local_addr + "]: who-has " + mbr);

        msg=new Message();  // bcast msg
        hdr=new FdHeader(FdHeader.WHO_HAS_SOCK);
        hdr.mbr=mbr;
        msg.putHeader(name, hdr);
        passDown(new Event(Event.MSG, msg));
    }


    /**
     Sends or broadcasts a I_HAVE_SOCK response. If 'dst' is null, the reponse will be broadcast, otherwise
     it will be unicast back to the requester
     */
    void sendIHaveSockMessage(Address dst, Address mbr, IpAddress addr) {
        Message msg=new Message(dst, null, null);
        FdHeader hdr=new FdHeader(FdHeader.I_HAVE_SOCK);
        hdr.mbr=mbr;
        hdr.sock_addr=addr;
        msg.putHeader(name, hdr);
        msg.isHighPriority = true;

        if(trace) // +++ remove
            log.trace("hdr=" + hdr);

        passDown(new Event(Event.MSG, msg));
    }
    
    // GemStoneAddition
    IpAddress fetchPingAddress(Address mbr) {
      return fetchPingAddress(mbr, this.connectTimeout);
    }


    /**
     Attempts to obtain the ping_addr first from the cache, then by unicasting q request to <code>mbr</code>,
     then by multicasting a request to all members.
     */
    public IpAddress fetchPingAddress(Address mbr, long timeout) {
        IpAddress ret;
        Message ping_addr_req;
        FdHeader hdr;

        if(mbr == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.FD_SOCK_MBR__NULL);
            return null;
        }
        // 1. Try to get from cache. Add a little delay so that joining mbrs can send their socket address before
        //    we ask them to do so
        ret=(IpAddress)cache.get(mbr);
        if(ret != null || timeout == 0) { // GemStoneAddition - timeout
            return ret;
        }

        try { // GemStoneAddition
          Util.sleep(300);
        }
        catch (InterruptedException e) {
          // Treat as a failure (as though the Promise failed)
          Thread.currentThread().interrupt();
          return null;
        }
        if((ret=(IpAddress)cache.get(mbr)) != null)
            return ret;


        // 2. Try to get from mbr
        Promise ping_addr_promise;
        synchronized (ping_addr_promises) {
          ping_addr_promise = ping_addr_promises.get(mbr);
          if (ping_addr_promise == null) {
            ping_addr_promise = new Promise();
            ping_addr_promises.put(mbr, ping_addr_promise);
          }
        }
        ping_addr_promise.reset();
        ping_addr_req=new Message(mbr, null, null); // unicast
        hdr=new FdHeader(FdHeader.WHO_HAS_SOCK);
        hdr.mbr=mbr;
        ping_addr_req.putHeader(name, hdr);
        passDown(new Event(Event.MSG, ping_addr_req));

// GemStoneAddition - don't wait yet.  Send msg to all and then wait for any response
//        ret=(IpAddress) ping_addr_promise.getResult(timeout);
//        if(ret != null) {
//            return ret;
//        }


        // 3. Try to get from all members
        ping_addr_req=new Message(null, null, null); // multicast
        hdr=new FdHeader(FdHeader.WHO_HAS_SOCK);
        hdr.mbr=mbr;
        ping_addr_req.putHeader(name, hdr);
        passDown(new Event(Event.MSG, ping_addr_req));
        ret=(IpAddress) ping_addr_promise.getResult(timeout);
        ping_addr_promises.remove(mbr);
        if (ret != null) { // GemStoneAddition - update the cache
          this.cache.put(mbr, ret);
        }
        return ret;
    }


    protected/*GemStoneAddition*/ Address determinePingDest() {
      // GemStoneAddition - copy the list and iterate over the copy
      List mbrs = null;
      synchronized(pingable_mbrs) {
        mbrs = new ArrayList(pingable_mbrs);
      }

      if(/*mbrs == null || */ mbrs.size() < 2 || local_addr == null)
          return null;

      int myIndex = mbrs.indexOf(local_addr);
      if (myIndex < 0) {
        return null;
      }
      
      // GemStoneAddition - broadcaster tracks suspects, which are in
      // mbrs list and must be skipped here, and also departed members
      int neighborIndex = myIndex;
      boolean wrapped = false;
      Address neighborAddr = null;
      boolean suspectMbr, departedMember;
      Vector skippedMbrs= null;
      do {
        suspectMbr = false; // GemStoneAddition
        neighborIndex++;
        if (neighborIndex > (mbrs.size()-1)) {
          neighborIndex = 0;
          wrapped = true;
        }
        if (wrapped && (neighborIndex == myIndex)) {
          neighborAddr = null;
          break;
        }
        neighborAddr = (Address)mbrs.get(neighborIndex);
        suspectMbr = bcast_task.isSuspectedMember(neighborAddr);
        departedMember = bcast_task.isDepartedMember(neighborAddr)
            /*|| stack.jgmm.isShuttingDown((IpAddress)neighborAddr)*/;
        if (suspectMbr) {
          if (skippedMbrs == null) {
            skippedMbrs = new Vector();
          }
          skippedMbrs.add(neighborAddr);
        }
      } while (suspectMbr || departedMember);

      // GemStoneAddition - bug #41772.  Notify coordinators of mbrs we
      // skipped when determining the ping-dest
      long currentTime = System.currentTimeMillis();
      if (currentTime > lastPingSelectionSuspectProcessingTime + this.suspect_msg_interval) {
        lastPingSelectionSuspectProcessingTime = currentTime;
        if (skippedMbrs != null && !(this.disconnecting || this.stack.getChannel().closing())) {
          FdHeader hdr=new FdHeader(FdHeader.SUSPECT);
          hdr.mbrs=skippedMbrs; 
          if (currentView != null) {
            hdr.vid = currentView.getVid();
          }
          // notify all potential coordinators that we've skipped some suspected/departed members
          for (IpAddress dest: ((List<IpAddress>)mbrs)) {
            if (dest.preferredForCoordinator()) {
              if (log.isDebugEnabled()) {
                log.debug("determinePingDest sending suspect message to " + dest + " suspects: " + skippedMbrs);
              }
              Message suspect_msg=new Message();
              suspect_msg.setDest(dest);
              suspect_msg.putHeader(name, hdr);
              passDown(new Event(Event.MSG, suspect_msg));
            }
          }
        }
      }

      return neighborAddr;
  }

    // GemStoneAddition - rewritten to not modify pingable_mbrs to remove
    // suspects.  suspects are kept only in the broadcast task now
//    Address determinePingDest() {
//        Address tmp;
//      synchronized(pingable_mbrs) { // gemstoneaddition
//        if(/*pingable_mbrs == null || GemStoneAddition (cannot be null) */ pingable_mbrs.size() < 2 || local_addr == null)
//            return null;
//        for(int i=0; i < pingable_mbrs.size(); i++) {
//            tmp=(Address) pingable_mbrs.elementAt(i);
//            if(local_addr.equals(tmp)) {
//                if(i + 1 >= pingable_mbrs.size())
//                    return (Address) pingable_mbrs.elementAt(0);
//                else
//                    return (Address) pingable_mbrs.elementAt(i + 1);
//            }
//        }
//        return null;
//      }
//    }


    Address determineCoordinator() {
      synchronized(members) {
        View cur = currentView;
        if (cur == null) {
          return null;
        } else {
          return cur.getCoordinator();
        }
      }
    }





    /* ------------------------------- End of Private Methods ------------------------------------ */


    public static class FdHeader extends Header implements Streamable {
        public boolean abnormalTermination; // GemStoneAddition
        public static final byte SUSPECT=10;
        public static final byte WHO_HAS_SOCK=11;
        public static final byte I_HAVE_SOCK=12;
        public static final byte GET_CACHE=13; // sent by joining member to coordinator
        public static final byte GET_CACHE_RSP=14; // sent by coordinator to joining member in response to GET_CACHE
        public static final byte NOT_MEMBER=15; // sent by coordinator to unknown member
        
        public static final byte FD_SUSPECT=20; // GemStoneAddition - for FD to askd FD_SOCK to try to (re)connect

        byte      type=SUSPECT;
        Address   mbr=null;           // set on WHO_HAS_SOCK (requested mbr), I_HAVE_SOCK, FD_SUSPECT
        IpAddress sock_addr;          // set on I_HAVE_SOCK

        // Hashtable<Address,IpAddress>
        Hashtable cachedAddrs=null;   // set on GET_CACHE_RSP
        Vector    mbrs=null;          // set on SUSPECT (list of suspected members)
        
        ViewId vid; // GemStoneAddition - the current view id for SUSPECT processing


        public FdHeader() {
        } // used for externalization

        public FdHeader(byte type) {
            this.type=type;
        }

        public FdHeader(byte type, Address mbr) {
            this.type=type;
            this.mbr=mbr;
        }

//        public FdHeader(byte type, Vector mbrs) {
//            this.type=type;
//            this.mbrs=mbrs;
//        }

        public FdHeader(byte type, Hashtable cachedAddrs) {
            this.type=type;
            this.cachedAddrs=cachedAddrs;
        }


        @Override // GemStoneAddition  
        public String toString() {
            StringBuffer sb=new StringBuffer();
            sb.append(type2String(type));
            if(mbr != null)
                sb.append(", mbr=").append(mbr);
            if(sock_addr != null)
                sb.append(", sock_addr=").append(sock_addr);
            if(cachedAddrs != null)
                sb.append(", cache=").append(cachedAddrs);
            if(mbrs != null)
                sb.append(", mbrs=").append(mbrs);
            sb.append(", abnormal=").append(this.abnormalTermination);
            return sb.toString();
        }


        public static String type2String(byte type) {
            switch(type) {
                case SUSPECT:
                    return "SUSPECT";
                case WHO_HAS_SOCK:
                    return "WHO_HAS_SOCK";
                case I_HAVE_SOCK:
                    return "I_HAVE_SOCK";
                case GET_CACHE:
                    return "GET_CACHE";
                case GET_CACHE_RSP:
                    return "GET_CACHE_RSP";
                case FD_SUSPECT:
                    return "FD_SUSPECT";
                default:
                    return "unknown type (" + type + ')';
            }
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(type);
            out.writeObject(mbr);
            out.writeObject(sock_addr);
            out.writeObject(cachedAddrs);
            out.writeObject(mbrs);
            out.writeBoolean(this.abnormalTermination);
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readByte();
            mbr=(Address) in.readObject();
            sock_addr=(IpAddress) in.readObject();
            cachedAddrs=(Hashtable) in.readObject();
            mbrs=(Vector) in.readObject();
            this.abnormalTermination = in.readBoolean();
        }

        @Override // GemStoneAddition  
        public long size(short version) {
            long retval=Global.BYTE_SIZE; // type
            retval+=Util.size(mbr,version);
            retval+=Util.size(sock_addr, version);

            retval+=Global.INT_SIZE; // cachedAddrs size
            Map.Entry entry;
            Address key;
            IpAddress val;
            if(cachedAddrs != null) {
                for(Iterator it=cachedAddrs.entrySet().iterator(); it.hasNext();) {
                    entry=(Map.Entry)it.next();
                    if((key=(Address)entry.getKey()) != null)
                        retval+=Util.size(key, version);
                    retval+=Global.BYTE_SIZE; // presence for val
                    if((val=(IpAddress)entry.getValue()) != null)
                        retval+=val.size(version);
                }
            }

            retval+=Global.INT_SIZE; // mbrs size
            if(mbrs != null) {
                for(int i=0; i < mbrs.size(); i++) {
                    retval+=Util.size((Address)mbrs.elementAt(i), version);
                }
            }
            
            retval++; // abnormal termination byte

            return retval;
        }

        public void writeTo(DataOutputStream out) throws IOException {
            int size;
            out.writeByte(type);
            Util.writeAddress(mbr, out);
            Util.writeStreamable(sock_addr, out);
            size=cachedAddrs != null? cachedAddrs.size() : 0;
            out.writeInt(size);
            if(size > 0) {
                for(Iterator it=cachedAddrs.entrySet().iterator(); it.hasNext();) {
                    Map.Entry entry=(Map.Entry)it.next();
                    Address key=(Address)entry.getKey();
                    IpAddress val=(IpAddress)entry.getValue();
                    Util.writeAddress(key, out);
                    Util.writeStreamable(val, out);
                }
            }
            size=mbrs != null? mbrs.size() : 0;
            out.writeInt(size);
            if(size > 0) {
                for(Iterator it=mbrs.iterator(); it.hasNext();) {
                    Address address=(Address)it.next();
                    Util.writeAddress(address, out);
                }
            }
            out.writeBoolean(this.abnormalTermination);
            Util.writeStreamable(vid, out); // GemStoneAddition
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            int size;
            type=in.readByte();
            mbr=Util.readAddress(in);
            sock_addr=(IpAddress)Util.readStreamable(IpAddress.class, in);
            size=in.readInt();
            if(size > 0) {
                if(cachedAddrs == null)
                    cachedAddrs=new Hashtable();
                for(int i=0; i < size; i++) {
                    Address key=Util.readAddress(in);
                    IpAddress val=(IpAddress)Util.readStreamable(IpAddress.class, in);
                    cachedAddrs.put(key, val);
                }
            }
            size=in.readInt();
            if(size > 0) {
                if(mbrs == null)
                    mbrs=new Vector();
                for(int i=0; i < size; i++) {
                    Address addr=Util.readAddress(in);
                    mbrs.add(addr);
                }
            }
            this.abnormalTermination = in.readBoolean(); // GemStoneAddition
            this.vid = (ViewId)Util.readStreamable(ViewId.class, in);
        }

    }

    public void beSick() { // GemStoneAddition
      if (!srv_sock_handler.beSick) {
        srv_sock_handler.beSick = true;
        stopServerSocket(false);
        FdHeader hdr=new FdHeader(FdHeader.SUSPECT);
        hdr.mbrs=new Vector();
        hdr.mbrs.add(this.local_addr);
        if (currentView != null) {
          hdr.vid = currentView.getVid();
        }
        Message suspect_msg=new Message();       // mcast SUSPECT to all members
        suspect_msg.putHeader(name, hdr);
        passDown(new Event(Event.MSG, suspect_msg));
      }
    }
    
    public void beHealthy() { // GemStoneAddition
      srv_sock_handler.beSick = false;
//      synchronized(srv_sock_handler.sicknessGuard) {
//        srv_sock_handler.sicknessGuard.notifyAll();
//      }
      // try to start up with the same address/port
      srv_sock=Util.createServerSocket(srv_sock_addr.getIpAddress(), srv_sock_addr.getPort(),
          65535);
      if (srv_sock_addr.getPort() != srv_sock.getLocalPort()) {
        // oops - couldn't get the same port. Things won't work if we don't tell others about it
        sendIHaveSockMessage(null, local_addr, srv_sock_addr);
      }
      srv_sock_addr=new IpAddress(srv_sock_bind_addr, srv_sock.getLocalPort());
      startServerSocket();
    }

    /**
     * Handles the server-side of a client-server socket connection. Waits until a client connects, and then loops
     * until that client closes the connection. Note that there is no new thread spawned for the listening on the
     * client socket, therefore there can only be 1 client connection at the same time. Subsequent clients attempting
     * to create a connection will be blocked until the first client closes its connection. This should not be a problem
     * as the ring nature of the FD_SOCK protocol always has only 1 client connect to its right-hand-side neighbor.
     */
    private class ServerSocketHandler implements Runnable {
      /**
       * Volatile reads OK, updates must be synchronized 
       */
      // @guarded.By this
        volatile Thread acceptor=null; 

        volatile boolean beSick; // GemStoneAddition - sickness simulation
        
        Object sicknessGuard = new Object(); // GemStoneAddition

        // GemStoneAddition (removal)
//        /** List of ClientConnectionHandler */
//        final List clients=new ArrayList();
        
        // GemStoneAddition
        /**
         * Controls updates to {@link #clients}
         */
        final Object clientsMutex = new Object();
        
        // GemStoneAddition
        /**
         * Volatile reads OK, updates must be synchronized
         */
        // @guarded.By clientsMutex
        volatile ClientConnectionHandler clients[] = new ClientConnectionHandler[0];

        ServerSocketHandler() {
//            start(); GemStoneAddition -- don't start  until srv_sock is set
        }

        synchronized /* GemStoneAddition */ void start() {
            if(acceptor == null ||  !acceptor.isAlive() /* GemStoneAddition */) {
                acceptor=new Thread(GemFireTracer.GROUP, this, "FD_SOCK listener thread");
                acceptor.setDaemon(true);
                acceptor.start();
            }
        }


        synchronized /* GemStoneAddition */ void stop(boolean normalTermination) {
            if(acceptor != null && acceptor.isAlive()) {
              acceptor.interrupt(); // GemStoneAddition
                try {
                    srv_sock.close(); // this will terminate thread, peer will receive SocketException (socket close)
                }
                catch(Exception ex) {
                }
            }
            // GemStoneAddition: rewrite to avoid iterators and
            // to facilitate emergencyClose
//            synchronized(clients) {
//                for(Iterator it=clients.iterator(); it.hasNext();) {
//                    ClientConnectionHandler handler=(ClientConnectionHandler)it.next();
//                    handler.stopThread();
//                }
//                clients.clear();
//            }
            synchronized (clientsMutex) {
              for (int i = 0; i < clients.length; i ++) {
                ClientConnectionHandler handler = clients[i];
                handler.stopThread(normalTermination);
              }
              clients = new ClientConnectionHandler[0];
            }
            acceptor=null;
        }

        // GemStoneAddition
        /**
         * Close the acceptor and the ClientConnectionHandler's, without
         * allocating any objects
         * 
         * @see SystemFailure#emergencyClose()
         * @see #stop()
         */
        void emergencyClose() {
          // Interrupt the thread, if any
          Thread thr = acceptor; // volatile fetch
          if(thr != null && thr.isAlive()) {
            acceptor.interrupt();
          }
          acceptor=null; // volatile write
          
          // Close the server socket, if any
          ServerSocket ss = srv_sock; // volatile fetch
          if (ss != null) {
            try {
              srv_sock.close(); // this will terminate thread, peer will receive SocketException (socket close)
            }
            catch (IOException ex) {
              // ignore
            }
          }
          
          ClientConnectionHandler snap[] = clients; // volatile fetch
          for (int i = 0; i < snap.length; i ++) {
            ClientConnectionHandler handler = snap[i];
            handler.emergencyClose();
          }
        }
        
        /** Only accepts 1 client connection at a time (saving threads) */
        public void run() {
            Socket client_sock;
            for (;;) { // GemStoneAddition -- avoid coding anti-pattern
              if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
                try {
                    if(trace) // +++ remove
                        log.trace("waiting for client connections on " + srv_sock.getInetAddress() + ":" +
                                  srv_sock.getLocalPort());
                    synchronized(this.sicknessGuard) {
                      if (this.beSick) { // GemStoneAddition - for testing
                        try {
                          log.getLogWriter().info(ExternalStrings.ONE_ARG, "FD_SOCK protocol will begin acting sick");
                          this.sicknessGuard.wait();
                        }
                        catch (InterruptedException ie) {
                          return;
                        }
                        log.getLogWriter().info(ExternalStrings.ONE_ARG, "FD_SOCK protocol is done acting sick");
                      }
                    }
                    client_sock=srv_sock.accept();
                    if(trace) // +++ remove
                        log.trace("accepted connection from " + client_sock.getInetAddress() + ':' + client_sock.getPort());
                    ClientConnectionHandler client_conn_handler=
                        new ClientConnectionHandler(client_sock, this);
                    // GemStoneAddition
                    synchronized(clientsMutex) {
//                      clients.add(client_conn_handler);
                      this.clients = (ClientConnectionHandler[])
                          Util.insert(this.clients, this.clients.length, 
                              client_conn_handler);
                    }
                    client_conn_handler.start();
                }
                catch(IOException io_ex2) {
                    break;
                }
            }
//            acceptor=null; GemStoneAddition
        }
    }



    /** Handles a client connection; multiple client can connect at the same time */
    private static class ClientConnectionHandler extends Thread  {
        /**
         * Volatile reads ok, updates must be synchronized
         *
         */
        //  @guarded.By mutex
        volatile Socket      client_sock=null;
        
        /**
         * @see #client_sock
         */
        final Object mutex=new Object();
        
        // GemStoneAddition
//        List clients=new ArrayList();
        
        /**
         * GemStoneAddition refer to list of clients via the
         * spawning handler
         * 
         * @see ServerSocketHandler#clients
         */
        final ServerSocketHandler myHandler;

        ClientConnectionHandler(Socket client_sock, 
               ServerSocketHandler h) {
            setName("FD_SOCK ClientConnectionHandler");
            setDaemon(true);
            this.client_sock=client_sock;
            this.myHandler = h;
        }

        void stopThread(boolean normalTermination) {
            synchronized(mutex) {
                if(normalTermination && client_sock != null) {
                    try {
                        OutputStream out=client_sock.getOutputStream();
                        out.write(NORMAL_TERMINATION);
                        out.flush(); // GemStoneAddition
                    }
                    catch (VirtualMachineError err) { // GemStoneAddition
                      // If this ever returns, rethrow the error.  We're poisoned
                      // now, so don't let this thread continue.
                      throw err;
                    }
                    catch(Throwable t) {
                    }
                }
            }
            this.interrupt(); // GemStoneAddition -- be more aggressive about stopping
            closeClientSocket();
        }

        // GemStoneAddition
        /**
         * Just make sure this class gets loaded.
         * 
         * @see SystemFailure#loadEmergencyClasses()
         */
        static void loadEmergencyClasses() {
          
        }
        
        // GemStoneAddition
        /**
         * Close the client socket and interrupt the receiver.
         * 
         * @see SystemFailure#emergencyClose()
         */
        void emergencyClose() {
          Socket s = client_sock; // volatile fetch
          if (s != null) {
            try {
              s.close();
            }
            catch (IOException e) {
              // ignore
            }
          }
          this.interrupt(); // GemStoneAddition -- be more aggressive about stopping
        }
        
        void closeClientSocket() {
            synchronized(mutex) {
                if(client_sock != null) {
                    try {
                        client_sock.close();
                    }
                    catch(Exception ex) {
                    }
//                    client_sock=null;  GemStoneAddition - common jgroups antipattern: do not null out variables on close
                }
                this.interrupt(); // GemStoneAddition - wake it up
            }
        }

        @Override // GemStoneAddition  
        public void run() {
          InputStream in;
            try {
                // GemStoneAddition - don't rely on exclusively on socket err to exit
                for (;;) {
                  synchronized(mutex) {
                    if(client_sock == null)
                        break;
                    in=client_sock.getInputStream();
                  }
                  int b = in.read();
                  if (b == PROBE_TERMINATION) {
                    synchronized(mutex) {
                      client_sock.getOutputStream().write(PROBE_TERMINATION);
                      client_sock.getOutputStream().flush();
                      client_sock.shutdownOutput();
                    }
                  }
                  if (b == ABNORMAL_TERMINATION || b == NORMAL_TERMINATION
                      || b == PROBE_TERMINATION) {  // GemStoneAddition - health probes
                    break;
                  }
                  if (Thread.currentThread().isInterrupted()) {
                      break; // GemStoneAddition
                  }
                } // for
            }
            catch(IOException io_ex1) {
            }
            finally {
                Socket sock = client_sock; // GemStoneAddition: volatile fetch
                if (sock != null && !sock.isClosed()) // GemStoneAddition
                  closeClientSocket();
                // synchronized(clients} { clients.remove(this); }
                // GemStoneAddition rewrite avoiding iterators
                synchronized (myHandler.clientsMutex) {
                  for (int i = 0; i < myHandler.clients.length; i ++) {
                    if (myHandler.clients[i] == this) {
                      myHandler.clients = (ClientConnectionHandler[])
                          Util.remove(myHandler.clients, i);
                      break;
                    }
                  } // for
                } // synchronized
            }
        }
    }
    
    
    private class RandomPingTask implements TimeScheduler.Task {
      Address lastPd = null;

      @Override
      public boolean cancelled() {
        return disconnecting; 
      }

      @Override
      public long nextInterval() {
        return connectTimeout * 10;
      }

      @Override
      public void run() {
        Address pd;
        if (lastPd == null) {
          pd = getRandomPingDest();
        } else {
          pd = getSequentialPingDest(lastPd);
        }
//        log.getLogWriterI18n().info(JGroupsStrings.DEBUG, ""+local_addr+" random check starting");
        if (pd == null) {
          lastPd = null;
        } else {
          while (pd != null) {
            lastPd = pd;
//            if (!stack.jgmm.isShuttingDown((IpAddress)pd)) {
              if (trace) log.trace("" + local_addr + " is performing random health check on " + pd);
              if (checkSuspect(pd, getName()+" tcp/ip health check")) {
                return;
              }
              bcast_task.addSuspectedMember(pd);
//            }
            pd = getSequentialPingDest(pd);
          }
        }
      }
    }

      protected Address getRandomPingDest() {
        Object current_dest = ping_dest; // GemStoneAddition
        
        // GemStoneAddition - copy the list and iterate over the copy
        List mbrs;
        synchronized(randomMembers) {
          mbrs = new ArrayList(randomMembers);
        }

        if(/*mbrs == null || */ mbrs.size() < 5 || local_addr == null)
          return null;

        int myIndex = mbrs.indexOf(local_addr);
        if (myIndex < 0) {
          return null;
        }
        mbrs.remove(myIndex);
        
        // GemStoneAddition - broadcaster tracks suspects, which are in
        // mbrs list and must be skipped here
        int resultIndex = new Random().nextInt(mbrs.size());
        int startIndex = resultIndex;
        boolean wrapped = false;
        Address resultAddr = null;
        do {
          resultIndex++;
          if (resultIndex > (mbrs.size()-1)) {
            resultIndex = 0;
            wrapped = true;
          }
          if (wrapped && (resultIndex > startIndex)) {
            resultAddr = null;
            break;
          }
          resultAddr = (Address)mbrs.get(resultIndex);
        } while (bcast_task.isSuspectedMember(resultAddr) || bcast_task.isDepartedMember(resultAddr));

        return resultAddr;
      }

      protected/*GemStoneAddition*/ Address getSequentialPingDest(Address fromPd) {
        // GemStoneAddition - copy the list and iterate over the copy
        List mbrs;
        synchronized(pingable_mbrs) {
          mbrs = new ArrayList(pingable_mbrs);
        }
        mbrs.remove(local_addr);

        if(/*mbrs == null || */ mbrs.size() < 2)
            return null;

        int lastIndex = mbrs.indexOf(fromPd);
        
        // GemStoneAddition - broadcaster tracks suspects, which are in
        // mbrs list and must be skipped here
        int neighborIndex = lastIndex;
        boolean wrapped = false;
        Address neighborAddr = null;
        do {
          neighborIndex++;
          if (neighborIndex > (mbrs.size()-1)) {
            neighborIndex = 0;
            wrapped = true;
          }
          if (wrapped && (neighborIndex == lastIndex)) {
            neighborAddr = null;
            break;
          }
          neighborAddr = (Address)mbrs.get(neighborIndex);
        } while (bcast_task.isSuspectedMember(neighborAddr) || bcast_task.isDepartedMember(neighborAddr));

        return neighborAddr;
      }

    /**
     * Task that periodically broadcasts a list of suspected members to the group. Goal is not to lose
     * a SUSPECT message: since these are bcast unreliably, they might get dropped. The BroadcastTask makes
     * sure they are retransmitted until a view has been received which doesn't contain the suspected members
     * any longer. Then the task terminates.
     */
    protected/*GemStoneAddition*/ class BroadcastTask implements TimeScheduler.Task {
        final Vector suspected_mbrs=new Vector(7);
        // GemStoneAddition: map<address,long> of normally departed members
        final Map departedMembers = new BoundedLinkedHashMap(1000); 
        boolean stopped=false; // GemStoneAddition - this variable is now ignored.  After it's started BT is always running
        boolean added=false; // GemStoneAddition

        // GemStoneAddition - keep from suspecting members that leave normally
        public synchronized void addDepartedMember(Address mbr) {
//          log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "addDepartedMember mbr=" + mbr, new Exception("addDepartedMember stack trace"));
          long now = System.currentTimeMillis();
          // timeout expired entries
          for (Iterator it=departedMembers.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry entry = (Map.Entry)it.next();
            Long birthtime = (Long)entry.getValue();
            if (birthtime.longValue() + stack.gfPeerFunctions.getShunnedMemberTimeout() < now) {
              it.remove();
            }
          }
          departedMembers.put(mbr, now);
        }
        
        // GemStoneAddition - keep from suspecting members that leave normally
        public boolean isDepartedMember(Address addr) {
          Long birthtime = (Long)departedMembers.get(addr);
          if (birthtime == null) {
            return false;
          }
          // after member-timeout ms we will start suspecting the departed member
          // again.  This needs to be done in order to avoid long recovery time
          // from loss of multiple consecutive processes in the view.
          long departedExpiryTime = birthtime.longValue() + stack.gfPeerFunctions.getMemberTimeout();
          boolean stillDeparted = departedExpiryTime > System.currentTimeMillis();
          if (!stillDeparted) {
            departedMembers.remove(addr);
          }
          return stillDeparted;
        }

        /** Adds a suspected member. Starts the task if not yet running */
        public void addSuspectedMember(Address mbr) {
            if(mbr == null) return;
            synchronized(this) {
              synchronized(members) {
                if(!members.contains(mbr)) return;
              }
              synchronized(suspected_mbrs) {
                  if(!suspected_mbrs.contains(mbr)) {
                      suspected_mbrs.addElement(mbr);
//                      if(log.isDebugEnabled()) log.debug("addSuspectMember mbr=" + mbr + " (size=" + suspected_mbrs.size() + ')', new Exception("addSuspectMember stack trace"));
                  }
                  if (!added) {
                    stopped = false;
                    added = true;
                    timer.add(this, true);
                  }
//                  if(stopped && suspected_mbrs.size() > 0) {
//                      stopped=false;
//                      timer.add(this, true);
//                  }
              }
            }
        }
        
        /** GemStoneAddition - see if a member is suspect */
        public boolean isSuspectedMember(Address mbr) {
          synchronized(suspected_mbrs) {
            return suspected_mbrs.contains(mbr);
          }
        }

        // GemStoneAddition - return whether removed from suspects
        public boolean removeSuspectedMember(Address suspected_mbr) {
            if(suspected_mbr == null) return false;
            if(log.isDebugEnabled()) log.debug("member is " + suspected_mbr);
            boolean result;
            synchronized(suspected_mbrs) {
                if (suspected_mbrs.removeElement(suspected_mbr)) {
                  if(suspected_mbrs.size() == 0)
                    stopped=true;
                  result = true;
                }
                else {
                  result = false;
                }
            }
            if (result) {
              // GemStoneAddition - must restore the member in pingable_mbrs
              // in the correct position
              synchronized(pingable_mbrs) {
                pingable_mbrs.clear();
                pingable_mbrs.addAll(members);
//                pingable_mbrs.removeAll(suspected_mbrs);
//                if (log.isDebugEnabled()) {
//                  log.debug("pingable_mbrs is now " + pingable_mbrs);
//                }
              }
            }
            return result;
        }


        public void removeAll() {
            synchronized(suspected_mbrs) {
                suspected_mbrs.removeAllElements();
                stopped=true;
            }
        }


        /**
         * Removes all elements from suspected_mbrs that are <em>not</em> in the new membership.
         * Also adjusts the departedMembers collection
         */
        public void adjustSuspectedMembers(Vector new_mbrship) {
            Address suspected_mbr;

            if(new_mbrship == null || new_mbrship.size() == 0) return;
            synchronized(suspected_mbrs) {
                for(Iterator it=suspected_mbrs.iterator(); it.hasNext();) {
                    suspected_mbr=(Address) it.next();
                    if(!new_mbrship.contains(suspected_mbr)) {
                        it.remove();
                        if(log.isDebugEnabled())
                            log.debug("removed " + suspected_mbr + " (size=" + suspected_mbrs.size() + ')');
                    }
                }
                if(suspected_mbrs.size() == 0)
                    stopped=true;
            }
            
            // GemStoneAddition: now make sure a departed mbr address hasn't been reused
            synchronized(this) {
              for (Iterator it=departedMembers.keySet().iterator(); it.hasNext(); ) {
                Address mbr = (Address)it.next();
                if (new_mbrship.contains(mbr)) {
                  it.remove();
                }
              }
            }
        }


        public boolean cancelled() {
          // cannot synchronize here to access 'stopped' because the
          // TimeScheduler locks the queue while invoking this method
          return false; // stopped  - GemStoneAddition - BT was being added to the timer multiple times
                        // sometimes many, many, many times resulting in many suspect messages
        }


        public long nextInterval() {
            return suspect_msg_interval;
        }


        public void run() {
//            Message suspect_msg;
            FdHeader hdr;
            
            synchronized(suspected_mbrs) {
                if(suspected_mbrs.size() == 0) {
                    stopped=true;
//                    if(log.isDebugEnabled()) log.debug("task done (no suspected members)");
                    return;
                }

//              if(log.isDebugEnabled())
                log.getLogWriter().info( ExternalStrings.
                    FD_SOCK_BROADCASTING_SUSPECT_MESSAGE_SUSPECTED_MBRS_0_TO_GROUP,
                    suspected_mbrs);

                hdr=new FdHeader(FdHeader.SUSPECT);
                hdr.mbrs=(Vector) suspected_mbrs.clone();
                if (currentView != null) {
                  hdr.vid = currentView.getVid();
                }
            }
            sendSuspectMessage(hdr, "still being suspected");
            if(log.isDebugEnabled()) log.debug("task done");
        }
    }

    /** GemStoneAddition - an object to monitor connection progress */
    class ConnectTimerTask extends TimerTask implements ConnectionWatcher {
      Socket watchedSocket;
      volatile boolean cancelled;
      
      public void beforeConnect(Socket sock) {
        watchedSocket = sock;
//        log.getLogWriter().info("DEBUG: beforeConnect, timeout="+timeout, new Exception("stack trace"));
        Timer t = stack.gfPeerFunctions.getConnectionTimeoutTimer();
        if (t != null) {
          t.schedule(this, FD_SOCK.this.connectTimeout);
        }
      }
      
      public void afterConnect(Socket sock) {
//        if (watchedSocket.isClosed()) {
//          log.getLogWriter().info("DEBUG: socket already closed in afterConnect");
//        }
        cancelled = true;
        this.cancel();
      }
      
      @Override // GemStoneAddition
      public void run() {
        if (!cancelled) {
          this.cancel();
          try {
            watchedSocket.close();
          }
          catch (IOException e) {
            // unable to close the socket - give up
          }
        }
      }
    
    }

}
