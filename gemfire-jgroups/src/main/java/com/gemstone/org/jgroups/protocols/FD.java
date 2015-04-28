/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: FD.java,v 1.31 2005/12/16 15:34:13 belaban Exp $

package com.gemstone.org.jgroups.protocols;



import java.util.concurrent.CopyOnWriteArrayList;
import com.gemstone.org.jgroups.*;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.*;

import java.io.*;
import java.util.*;
import java.util.List;


/**
 * Failure detection based on simple heartbeat protocol. Regularly polls members for
 * liveness. Multicasts SUSPECT messages when a member is not reachable. The simple
 * algorithms works as follows: the membership is known and ordered. Each HB protocol
 * periodically sends an 'are-you-alive' message to its *neighbor*. A neighbor is the next in
 * rank in the membership list, which is recomputed upon a view change. When a response hasn't
 * been received for n milliseconds and m tries, the corresponding member is suspected (and
 * eventually excluded if faulty).<p>
 * FD starts when it detects (in a view change notification) that there are at least
 * 2 members in the group. It stops running when the membership drops below 2.<p>
 * When a message is received from the monitored neighbor member, it causes the pinger thread to
 * 'skip' sending the next are-you-alive message. Thus, traffic is reduced.<p>
 * When we receive a ping from a member that's not in the membership list, we shun it by sending it a
 * NOT_MEMBER message. That member will then leave the group (and possibly rejoin). This is only done if
 * <code>shun</code> is true.
 * @author Bela Ban
 * @version $Revision: 1.31 $
 */
public class FD extends Protocol  {
    volatile Address               ping_dest=null;  // GemStoneAddition - volatile
    Address               local_addr=null;
    long                  timeout=3000;  // number of millisecs to wait for an are-you-alive msg
    volatile long                  last_ack=System.currentTimeMillis(); // GemStoneAddition - volatile
    volatile int                   num_tries=0; // GemStoneAddition - volatile
    int                   max_tries=2;   // number of times to send a are-you-alive msg (tot time= max_tries*timeout)
    final List            members=new CopyOnWriteArrayList();
    Address coordinator; // GemStoneAddition
    final Hashtable       invalid_pingers=new Hashtable(7);  // keys=Address, val=Integer (number of pings from suspected mbrs)

    /** Members from which we select ping_dest. may be subset of {@link #members} */
    final List            pingable_mbrs=new CopyOnWriteArrayList();

    boolean               shun=true;
    TimeScheduler         timer=null;
    Monitor               monitor=null;  // task that performs the actual monitoring for failure detection
    private final Object  monitor_mutex=new Object();
    protected/*GemStoneAddition*/ int           num_heartbeats=0;
    protected/*GemStoneAddition*/ int           num_suspect_events=0;

    /** Transmits SUSPECT message until view change or UNSUSPECT is received */
    final Broadcaster     bcast_task=new Broadcaster();
    final static String   name="FD";

    BoundedList           suspect_history=new BoundedList(20);


    /** GemStoneAddition active heartbeat_ack sender task */
    HeartbeatSender     hbsender = null;
    
    boolean beingSick; // GemStoneAddition - test hook
    static boolean DISABLED = Boolean.getBoolean("gemfire.DISABLE_FD"); // GemStoneAddition



    @Override // GemStoneAddition  
    public String getName() {return name;}
    public String getLocalAddress() {return local_addr != null? local_addr.toString() : "null";}
    public String getMembers() {return members != null? members.toString() : "null";}
    public String getPingableMembers() {return pingable_mbrs != null? pingable_mbrs.toString() : "null";}
    public String getPingDest() {return ping_dest != null? ping_dest.toString() : "null";}
    public int getNumberOfHeartbeatsSent() {return num_heartbeats;}
    public int getNumSuspectEventsGenerated() {return num_suspect_events;}
    public long getTimeout() {return timeout;}
    public void setTimeout(long timeout) {this.timeout=timeout;}
    public int getMaxTries() {return max_tries;}
    public void setMaxTries(int max_tries) {this.max_tries=max_tries;}
    public int getCurrentNumTries() {return num_tries;}
    public boolean isShun() {return shun;}
    public void setShun(boolean flag) {this.shun=flag;}
    public String printSuspectHistory() {
        StringBuffer sb=new StringBuffer();
        for(Enumeration en=suspect_history.elements(); en.hasMoreElements();) {
            sb.append(new Date()).append(": ").append(en.nextElement()).append("\n");
        }
        return sb.toString();
    }


    @Override // GemStoneAddition  
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("timeout");
        if(str != null) {
            timeout=Long.parseLong(str);
            props.remove("timeout");
        }

        str=props.getProperty("max_tries");  // before suspecting a member
        if(str != null) {
            max_tries=Integer.parseInt(str);
            props.remove("max_tries");
        }

        str=props.getProperty("shun");
        if(str != null) {
            shun=Boolean.valueOf(str).booleanValue();
            props.remove("shun");
        }

        if(props.size() > 0) {
            log.error(ExternalStrings.FD_FDSETPROPERTIES_THE_FOLLOWING_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);

            return false;
        }
        return true;
    }

    @Override // GemStoneAddition  
    public void resetStats() {
        num_heartbeats=num_suspect_events=0;
        suspect_history.removeAll();
    }


    @Override // GemStoneAddition  
    public void init() throws Exception {
        if(stack != null && stack.timer != null)
            timer=new TimeScheduler(60000); // GemStoneAddition: run monitor in a separate 
                                            // timer since it can take a while to run
        else
            throw new Exception(getName()+".init(): timer cannot be retrieved from protocol stack");
    }


    /**
     * Just ensure that this class gets loaded.
     * 
     * @see SystemFailure#loadEmergencyClasses()
     */
    public static void loadEmergencyClasses() { // GemStoneAddition
      // no further action required
    }
    
    /**
     * Kill the Monitor and the HeartbeadSender
     * 
     * @see SystemFailure#emergencyClose()
     */
    public void emergencyClose() { // GemStoneAddition
//      stop();

      Monitor m = monitor;
      if (m != null) {
        m.stop();
      }
      HeartbeatSender hb = this.hbsender;
      if (hb != null) {
        hb.stop();
      }
    }
    
    @Override // GemStoneAddition  
    public void stop() {
        stopMonitor();
    }


    protected/*GemStoneAddition*/ Address getPingDest(List mbrs) {
        Object current_dest = ping_dest; // GemStoneAddition
        
        // GemStoneAddition - copy the list and iterate over the copy
        synchronized(mbrs) {
          mbrs = new ArrayList(mbrs);
        }

        if(/*mbrs == null || */ mbrs.size() < 2 || local_addr == null)
            return null;

        int myIndex = mbrs.indexOf(local_addr);
        if (myIndex < 0) {
          return null;
        }
        
        // GemStoneAddition - broadcaster tracks suspects, which are in
        // mbrs list and must be skipped here
        int neighborIndex = myIndex;
        boolean wrapped = false;
        Address neighborAddr = null;
        do {
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
        } while (bcast_task.isSuspectedMember(neighborAddr));

        // GemStoneAddition - reset timestamp and ack count if we change ping_dest
        // to avoid immediately suspecting new member when the change happened due
        // to a member being ejected
        if (current_dest != null  &&  neighborAddr != null  &&  !current_dest.equals(neighborAddr)) {
          last_ack = System.currentTimeMillis();
          num_tries = 0;
        }
        return neighborAddr;
    }


    /** GemStoneAddition - active heartbeat destination determination */
    Address getHeartbeatDest() {
      List mbrs;
      
      synchronized(pingable_mbrs) {
        mbrs = new ArrayList(pingable_mbrs);
      }

      int myIndex = mbrs.indexOf(local_addr);
      if (myIndex == 0) {
        return null;
      }
      
      // GemStoneAddition - broadcaster tracks suspects, which are in
      // mbrs list and must be skipped here
      int neighborIndex = myIndex;
      boolean wrapped = false;
      Address neighborAddr = null;
      do {
        neighborIndex--;
        if (neighborIndex < 0) {
          neighborIndex = mbrs.size()-1;
          wrapped = true;
        }
        if (wrapped && (neighborIndex == myIndex)) {
          neighborAddr = null;
          break;
        }
        neighborAddr = (Address)mbrs.get(neighborIndex);
      } while (bcast_task.isSuspectedMember(neighborAddr));

      return neighborAddr;
    }


    private void startMonitor() {
      if (DISABLED || disconnecting) {
        return;
      }
        synchronized(monitor_mutex) {
            if(monitor != null && monitor.started == false) {
                monitor=null;
            }
            if(monitor == null) {
                monitor=new Monitor();
                last_ack=System.currentTimeMillis();  // start from scratch
                num_tries=0;  // GemStoneAddition - initialize this before scheduling the monitor
                timer.add(monitor, true);  // fixed-rate scheduling
            }
            // GemStoneAddition - start heartbeat sender task
            if (this.hbsender != null && this.hbsender.started == false) {
              this.hbsender = null;
            }
            if (this.hbsender == null) {
              this.hbsender = new HeartbeatSender();
              // run the hb sender in the stack's timer so it isn't blocked by the Monitor
              stack.timer.add(this.hbsender, true);
            }
        }
    }

    private void stopMonitor() {
        synchronized(monitor_mutex) {
            if(monitor != null) {
                monitor.stop();
                monitor=null;
            }
            // GemStoneAddition - stop heartbeat sender task
            if (this.hbsender != null) {
              this.hbsender.stop();
              this.hbsender = null;
            }
        }
    }

    private boolean isCoordinator; // GemStoneAddition
    private boolean disconnecting; // GemStoneAddition

    @Override // GemStoneAddition  
    public void up(Event evt) {
        Message msg;
        FdHeader hdr;
        Object sender, tmphdr;
        // GemStoneAddition - avoid race conditions by reading ping_dest and caching it
        Address pd = ping_dest;

        switch(evt.getType()) {

        case Event.SET_LOCAL_ADDRESS:
            local_addr=(Address)evt.getArg();
            break;
            
        case Event.MSG:
          if (DISABLED || disconnecting) {
            break;
          }
            msg=(Message)evt.getArg();
            // GemStoneAddition - check for mismatched configuration with FD_SOCK
            
            tmphdr=msg.getHeader(getName());
            if(tmphdr == null || !(tmphdr instanceof FdHeader)) {
                if(pd != null && (sender=msg.getSrc()) != null) {
                    if(pd.equals(sender)) {
                        last_ack=System.currentTimeMillis();
//                        if(trace)
//                            log.trace("received msg from " + sender + " (counts as heartbeat)");
                        num_tries=0;
                    }
                }
                break;  // message did not originate from FD layer, just pass up
            }

            hdr=(FdHeader)msg.removeHeader(getName());
            switch(hdr.type) {
            case FdHeader.HEARTBEAT:                       // heartbeat request; send heartbeat ack
              if (this.beingSick) { // GemStoneAddition - test hook
                break;
              }
                Address hb_sender=msg.getSrc();
                Message hb_ack=new Message(hb_sender, null, null);
                hb_ack.isHighPriority = true;
                FdHeader tmp_hdr=new FdHeader(FdHeader.HEARTBEAT_ACK);

                // 1.  Send an ack
                tmp_hdr.from=local_addr;
                hb_ack.putHeader(getName(), tmp_hdr);
                if(trace)
                    log.trace(getLocalAddress() + ":" + getName() + " received heartbeat request from " + hb_sender + ", sending heartbeat");
                passDown(new Event(Event.MSG, hb_ack));

                // 2. Shun the sender of a HEARTBEAT message if that sender is not a member. This will cause
                //    the sender to leave the group (and possibly rejoin it later)
                if(shun)
                    shunInvalidHeartbeatSender(hb_sender);
                break;                                     // don't pass up !

            case FdHeader.HEARTBEAT_ACK:                   // heartbeat ack
                if(pd != null && pd.equals(hdr.from)) {
                    last_ack=System.currentTimeMillis();
                    num_tries=0;
                    if(log.isDebugEnabled()) log.debug(getLocalAddress() + ":" + getName() + " received heartbeat from " + hdr.from);
                }
                else {
                    stop();
                    if (log.isDebugEnabled()) log.debug(getLocalAddress() + ":" + getName() + " received heartbeat from " + hdr.from + " who is not my ping-dest (" + pd + ")");
                    ping_dest=getPingDest(pingable_mbrs);
                    pd = ping_dest;
                    if(pd != null) {
                        try {
                            startMonitor();
                        }
                        catch(Exception ex) {
                            if(warn) log.warn(ExternalStrings.FD_EXCEPTION_WHEN_CALLING_STARTMONITOR, ex);
                        }
                    }
                    if (log.isDebugEnabled()) log.debug(getLocalAddress() + ":" + getName() + " ping_dest is now " + pd);
                }
                break;

            case FdHeader.SUSPECT:
                if(hdr.mbrs != null) {
                    if(trace) log.trace("[SUSPECT] suspect hdr is " + hdr);
                    // GemStoneAddition - log the notification
//                    log.getLogWriterI18n().info(
//                      JGroupsStrings.FD_RECEIVED_SUSPECT_NOTIFICATION_FOR_MEMBERS_0_FROM_1_2,
//                      new Object[] {hdr.mbrs, msg.getSrc(), ""});
                    // GemStoneAddition - if the sender isn't in this member's view,
                    // and this is the coordinator, he may have been ousted from
                    // the system and should be told so
                    if (!isInMembership(msg.getSrc())) {
                      break;
                    }
                    for(int i=0; i < hdr.mbrs.size(); i++) {
                        Address m=(Address)hdr.mbrs.elementAt(i);
                        if(local_addr != null && m.equals(local_addr)) {
                            if(warn)
                                log.warn("I was suspected, but will not remove myself from membership " +
                                         "(waiting for EXIT message)");
                        }
                        else {
                          // GemStoneAddition - broadcaster tracks suspects, and
                          // they are not removed from pingable_mbrs
                          bcast_task.addSuspectedMember(m);
//                          synchronized(pingable_mbrs) { // GemStoneAddition - synch on this
//                            pingable_mbrs.remove(m);
//                          }
                            ping_dest=getPingDest(pingable_mbrs);
                            if (log.isDebugEnabled()) log.debug("Old "+getName()+" ping-dest was susepected, so selected new ping-dest " + ping_dest);
                            pd = ping_dest;
                            if (pd != null) { // GemStoneAddition - start the monitor
                              try {
                                startMonitor();
                              } catch (Exception ex) {
                                if (warn) log.warn("exception when calling startMonitor()", ex);
                              }
                            }
                        }
                        passUp(new Event(Event.SUSPECT, new SuspectMember(msg.getSrc(), m))); // GemStoneAddition SuspectMember struct
                        passDown(new Event(Event.SUSPECT, new SuspectMember(msg.getSrc(), m)));
                    }
                }
                break;

            case FdHeader.NOT_MEMBER:
                if(shun) {
                    log.getLogWriter().severe(ExternalStrings.FD_RECEIVED_NOT_MEMBER_MESSAGE_FROM_0_THIS_VM_IS_NO_LONGER_A_MEMBER_EXITING, msg.getSrc());
                    passUp(new Event(Event.EXIT, stack.gfBasicFunctions.getForcedDisconnectException(
                      ExternalStrings.FD_THIS_MEMBER_HAS_BEEN_FORCED_OUT_OF_THE_DISTRIBUTED_SYSTEM_PLEASE_CONSULT_GEMFIRE_LOGS_TO_FIND_THE_REASON_FD.toLocalizedString())));
                }
                break;
            }
//            return; GemStoneAddition - let VERIFY_SUSPECT see this traffic
        }
        passUp(evt); // pass up to the layer above us
    }

    
    public void beSick() { // GemStoneAddition
      this.beingSick = true;
    }
    
    public void beHealthy() { // GemStoneAddition
      this.beingSick = false;
    }

    /**
     * GemStoneAddition - allows notification of msg being received from
     * a member through GemFire's other communication channels
     * 
     * @param sender the address that sent the message
     */
    public void messageReceivedFrom(Address sender) {
      if (DISABLED || disconnecting) {
        return;
      }
      Address pd = ping_dest;
      if(pd != null  &&  pd.equals(sender)) {
        last_ack=System.currentTimeMillis();
//        if(trace)
//            log.trace("FD received msg from " + sender + " (counts as heartbeat)");
        num_tries=0;
      }
    }


    
    @Override // GemStoneAddition  
    public void down(Event evt) {
        View v;
        // GemStoneAddtition - avoid race conditions by reading ping_dest once and caching it
        Address pd = ping_dest;

        switch(evt.getType()) {
        case Event.MSG:
          if (DISABLED || disconnecting) {
            passDown(evt);
            break;
          }
          Message msg = (Message)evt.getArg();
          FD_SOCK.FdHeader hdr = (FD_SOCK.FdHeader)msg.getHeader("FD_SOCK");
          if (hdr != null && hdr.type == FD_SOCK.FdHeader.SUSPECT
              && hdr.mbrs.contains(pd)) {
            // my ping_dest has been suspected by FD_SOCK, so go on to the
            // next
            // GemStoneAddition - bcaster tracks all suspected members now
            for (Iterator it=hdr.mbrs.iterator(); it.hasNext(); ) {
              bcast_task.addSuspectedMember((Address)it.next());
            }
//            synchronized (pingable_mbrs) {
//              pingable_mbrs.removeAll(hdr.mbrs); 
//            }
            passDown(evt);
            ping_dest=getPingDest(pingable_mbrs);
            pd = ping_dest;
            if (log.isDebugEnabled()) log.debug(getLocalAddress() + ": " + getName() + " ping-dest is now " + pd);
            if (pd != null) {
              try {
                startMonitor();
              } catch (Exception ex) {
                if (warn) {
                  log.warn(ExternalStrings.FD_EXCEPTION_WHEN_CALLING_STARTMONITOR, ex);
                }
              }
            }
          }
          else {
            passDown(evt);
          }
          break;
            
        case Event.VIEW_CHANGE:
          if (DISABLED || disconnecting) {
            passDown(evt);
            break;
          }
            synchronized(this) {
                stop();
                v=(View)evt.getArg();
                this.coordinator = v.getCreator(); // GemStoneAddition - send heartbeat to coordinator, too
                members.clear();
                members.addAll(v.getMembers());
                bcast_task.adjustSuspectedMembers(members);
                synchronized(pingable_mbrs) {
                  Address coord = new Membership(v.getMembers()).getCoordinator(); // GemStoneAddition
                  this.isCoordinator = this.local_addr != null
                    && coord != null
                    && this.local_addr.equals(coord);
                  pingable_mbrs.clear();
                  pingable_mbrs.addAll(members);
                }
                passDown(evt);
                ping_dest=getPingDest(pingable_mbrs);
                if (log.isDebugEnabled()) log.debug(getLocalAddress()+":"+getName()+" ping-dest is now " + ping_dest + " and coordinator is " + coordinator);
                pd = ping_dest;
                if(pd != null) {
                    try {
                        startMonitor();
                    }
                    catch(Exception ex) {
                        if(warn) log.warn("exception when calling startMonitor()", ex);
                    }
                }
            }
            break;

        case Event.UNSUSPECT:
          if (DISABLED || disconnecting) {
            passDown(evt);
            break;
          }
          Address mbr = (Address)evt.getArg();
          if (log.isDebugEnabled()) {
            StringBuffer sb = new StringBuffer(getName()+" is unsuspecting ").append(mbr);
            log.getLogWriter().info(ExternalStrings.DEBUG, sb);
          }
            unsuspect(mbr);
            // GemStoneAddition - select ping_dest here instead of in unsuspect()
            ping_dest=getPingDest(pingable_mbrs);
            if (log.isDebugEnabled()) {
              StringBuffer sb = new StringBuffer(getLocalAddress()+":"+getName()+" ping-dest is now ").append(ping_dest);
              log.getLogWriter().info(ExternalStrings.DEBUG, sb);
            }
            pd = ping_dest;
            if (pd != null) { // GemStoneAddition - start the monitor
              try {
                startMonitor();
              } catch (Exception ex) {
                if (warn) log.warn("exception when calling startMonitor()", ex);
              }
            }
            passDown(evt);
            break;

        case Event.DISCONNECTING: // GemStoneAddition - make sure we stop shunning/suspecting at this point
          this.disconnecting = true;
          passDown(evt);
          stop();
          break;
          
        case Event.START: // GemStoneAddition - reset state when restarting
          this.disconnecting = false;
          passDown(evt);
          break;

        default:
            passDown(evt);
            break;
        }
    }


    private void unsuspect(Address mbr) {
        bcast_task.removeSuspectedMember(mbr);
//        synchronized(pingable_mbrs) { // GemStoneAddition - synch on this
//          pingable_mbrs.clear();
//          pingable_mbrs.addAll(members);
          // GemStoneAddition - pingable_mbrs contains all members, both suspect and non-suspect
//          pingable_mbrs.removeAll(bcast_task.getSuspectedMembers());
//        }
//        if (log.isDebugEnabled()) log.debug("unsuspected " + mbr + " in FD.  ping-dest is now " + ping_dest);
    }


    
    /**
     * GemStoneAddition if this is the coordinator, see if the member is in the
     * current view.  Otherwise punt and say he is in the view
     */
    private boolean isInMembership(Address sender) {
      if (this.isCoordinator) {
        if (pingable_mbrs != null) {
          synchronized(pingable_mbrs) {
            Set m = new HashSet(pingable_mbrs);
            return m.contains(sender);
          }
        }
      }
      return true;
    }
    
    public void SUSPECT_ALL() {
      log.getLogWriter().severe(ExternalStrings.ONE_ARG, getName()+".SUSPECT_ALL invoked", new Exception("stack trace"));
      synchronized(pingable_mbrs) {
        for (Iterator it=pingable_mbrs.iterator(); it.hasNext(); ) {
          Address mbr = (Address)it.next();
          if (!mbr.equals(this.local_addr)) {
            Message msg = new Message();
            FD_SOCK.FdHeader hdr = new FD_SOCK.FdHeader(FD_SOCK.FdHeader.FD_SUSPECT, mbr);
            msg.putHeader("FD_SOCK", hdr);
            passUp(new Event(Event.MSG, msg));
          }
        }
      }
    }
        
    /**
     * If sender is not a member, send a NOT_MEMBER to sender (after n pings received)
     */
    private void shunInvalidHeartbeatSender(Address hb_sender) {
        int num_pings=0;
        Message shun_msg;
        
        // GemStoneAddition - access members under sync
        boolean notMember;
        synchronized (this) {
          notMember = hb_sender != null && members != null && !members.contains(hb_sender);
        }

        if(notMember) {
            if(invalid_pingers.containsKey(hb_sender)) {
                num_pings=((Integer)invalid_pingers.get(hb_sender)).intValue();
                if(num_pings >= max_tries) {
                    if(log.isDebugEnabled())
                        log.debug(hb_sender + " is not in " + members + " ! Shunning it");
                    shun_msg=new Message(hb_sender, null, null);
                    shun_msg.putHeader(getName(), new FdHeader(FdHeader.NOT_MEMBER));
                    shun_msg.isHighPriority = true;
                    passDown(new Event(Event.MSG, shun_msg));
                    invalid_pingers.remove(hb_sender);
                }
                else {
                    num_pings++;
                    invalid_pingers.put(hb_sender, Integer.valueOf(num_pings));
                }
            }
            else {
                num_pings++;
                invalid_pingers.put(hb_sender, Integer.valueOf(num_pings));
            }
        }
    }


    public static class FdHeader extends Header implements Streamable {
        public static final byte HEARTBEAT=0;
        public static final byte HEARTBEAT_ACK=1;
        public static final byte SUSPECT=2;
        public static final byte NOT_MEMBER=3;  // received as response by pinged mbr when we are not a member


        byte    type=HEARTBEAT;
        Vector  mbrs=null;
        Address from=null;  // member who detected that suspected_mbr has failed



        public FdHeader() {
        } // used for externalization

        public FdHeader(byte type) {
            this.type=type;
        }

        public FdHeader(byte type, Vector mbrs, Address from) {
            this(type);
            this.mbrs=mbrs;
            this.from=from;
        }


        @Override // GemStoneAddition  
        public String toString() {
            switch(type) {
                case HEARTBEAT:
                    return "[FD: heartbeat request]";
                case HEARTBEAT_ACK:
                    return "[FD: heartbeat]";
                case SUSPECT:
                    return "[FD: SUSPECT (suspected_mbrs=" + mbrs + ", from=" + from + ")]";
                case NOT_MEMBER:
                    return "[FD: NOT_MEMBER]";
                default:
                    return "[FD: unknown type (" + type + ")]";
            }
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(type);
            if(mbrs == null)
                out.writeBoolean(false);
            else {
                out.writeBoolean(true);
                out.writeInt(mbrs.size());
                for(Iterator it=mbrs.iterator(); it.hasNext();) {
                    Address addr=(Address)it.next();
                    Marshaller.write(addr, out);
                }
            }
            Marshaller.write(from, out);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readByte();
            boolean mbrs_not_null=in.readBoolean();
            if(mbrs_not_null) {
                int len=in.readInt();
                mbrs=new Vector(11);
                for(int i=0; i < len; i++) {
                    Address addr=(Address)Marshaller.read(in);
                    mbrs.add(addr);
                }
            }
            from=(Address)Marshaller.read(in);
        }


        @Override // GemStoneAddition  
        public long size(short version) {
            int retval=Global.BYTE_SIZE; // type
            retval+=Util.size(mbrs, version);
            retval+=Util.size(from, version);
            return retval;
        }


        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
            Util.writeAddresses(mbrs, out);
            Util.writeAddress(from, out);
        }



        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readByte();
            mbrs=(Vector)Util.readAddresses(in, Vector.class);
            from=Util.readAddress(in);
        }

    }


    /**
     * GemStoneAddition - for idle processes, send a heartbeat ack every once
     * in a while to keep the other process from having to send a request for
     * one.
     */
    protected class HeartbeatSender implements TimeScheduler.Task {

      volatile /* GemStoneAddition */ boolean started = true;
      long interval = FD.this.timeout * 2 / 3;

      public void stop() {
        started = false;
      }
      
      public boolean cancelled() {
        return !started;
      }
      public long nextInterval() {
        return (interval <= 0? 25 : interval);
      }
      
      @Override // GemStoneAddition  
      public String toString() {
          return getName()+" heartbeat sender: " + started;
      }

      public void run() {
        if (!beingSick) {
          Address receiver = FD.this.getHeartbeatDest();
          Address coord = coordinator;
          if (log.isDebugEnabled()) {
            log.debug("sending heartbeat to " + receiver + " and coordinator " + coord);
          }
          if (receiver != null) {
            Message msg = new Message();
            msg.setDest(receiver);
            msg.putHeader(getName(), new FdHeader(FdHeader.HEARTBEAT_ACK,
                null, FD.this.local_addr));
            msg.isHighPriority = true;
            if (!started) return; // GemStoneAddition -- last-chance check
            FD.this.passDown(new Event(Event.MSG, msg));
          }
          // GemStoneAddition - also send a heartbeat to the coordinator
          if (coord != null) {
            Message msg = new Message();
            msg.setDest(coord);
            msg.putHeader(getName(), new FdHeader(FdHeader.HEARTBEAT_ACK,
                null, FD.this.local_addr));
            msg.isHighPriority = true;
            if (!started) return; // GemStoneAddition -- last-chance check
            FD.this.passDown(new Event(Event.MSG, msg));
          }
        }
      }
    }
    
    protected/*GemStoneAddition*/ class Monitor implements TimeScheduler.Task {
        volatile /* GemStoneAddition */ boolean started=true;

        public void stop() {
            started=false;
        }


        public boolean cancelled() {
            return !started;
        }


        /** this is the number of milliseconds until the task should be run again */
        public long nextInterval() {
          return timeout;
        }


        public void run() {
            Message hb_req;
            long not_heard_from; // time in msecs we haven't heard from ping_dest
            // GemStoneAddition - avoid race conditions by reading ping_dest only once
            Address pd = ping_dest;

            if (beingSick) {
              return;
            }
            
            if(pd == null) {
                // GemStoneAddition - changed from warn() to debug() since we now remove
                // suspected mbrs from pingable_members and recalculate ping_dest in
                // this method
                if(log.isDebugEnabled())
                    log.debug("ping_dest is null: members=" + members + ", pingable_mbrs=" +
                            pingable_mbrs + ", local_addr=" + local_addr);
                return;
            }


            // 1. send heartbeat request
            hb_req=new Message(pd, null, null);
            hb_req.putHeader(getName(), new FdHeader(FdHeader.HEARTBEAT));  // send heartbeat request
            hb_req.isHighPriority = true;
//            if(log.isDebugEnabled())
//                log.debug("sending heartbeat request to " + pd + " (own address=" + local_addr + ')'); // GemStoneAddition - this said "are-you-alive msg"
            if (!started) return; // GemStoneAddition
            passDown(new Event(Event.MSG, hb_req));
            num_heartbeats++;
            
            // 2. If the time of the last heartbeat is > timeout and max_tries heartbeat messages have not been
            //    received, then broadcast a SUSPECT message. Will be handled by coordinator, which may install
            //    a new view
            not_heard_from=System.currentTimeMillis() - last_ack;
            // quick & dirty fix: increase timeout by 500msecs to allow for latency (bela June 27 2003)
//            if(log.isDebugEnabled())
//              log.debug("FD running in " + local_addr + ":"+getName()+" watching " + pd +
//                      " not_heard_from=" + not_heard_from + " timeout=" + (timeout+500) +
//                      " num_tries=" + num_tries + " max_tries="+max_tries);
            if(not_heard_from > timeout + 500) { // no heartbeat ack for more than timeout msecs
                if(num_tries >= max_tries) {
                    if(log.isDebugEnabled())
                        log.debug("[" + local_addr + "]:"+getName()+" received no heartbeat ack from " + pd +
                                " for " + (num_tries +1) + " times (" + ((num_tries+1) * timeout) +
                                " milliseconds), suspecting it");
                    // broadcast a SUSPECT message to all members - loop until
                    // unsuspect or view change is received
                    //bcast_task.addSuspectedMember(pd);
                    FD_SOCK fdsock = (FD_SOCK)stack.findProtocol("FD_SOCK");
                    if (fdsock != null && !fdsock.checkSuspect(pd, getName()+" heartbeat timeout")) {
                      // GemStoneAddition - add to suspected mbrs and recalc ping_dest
                      synchronized(pingable_mbrs) {
                        //pingable_mbrs.remove(pd);
                        bcast_task.addSuspectedMember(pd);
                        ping_dest = getPingDest(pingable_mbrs);
                        if (log.isDebugEnabled()) log.debug(getLocalAddress()+":"+getName()+" ping-dest is now suspect.  new ping-dest is " + ping_dest);
                        if (ping_dest == null) {
                          stop();
                        }
                      }
                    }
                    num_tries=0;
                    if(stats) {
                        num_suspect_events++;
                        suspect_history.add(pd);
                    }
                }
                else {
                    if(log.isDebugEnabled())
                        log.debug("heartbeat missing from " + pd + " (number=" + num_tries + ')');
                    num_tries++;
                }
            }
        }


        @Override // GemStoneAddition  
        public String toString() {
            return getName()+" heartbeat monitor: " + started;
        }

    }


    /**
     * Task that periodically broadcasts a list of suspected members to the group. Goal is not to lose
     * a SUSPECT message: since these are bcast unreliably, they might get dropped. The BroadcastTask makes
     * sure they are retransmitted until a view has been received which doesn't contain the suspected members
     * any longer. Then the task terminates.
     */
    protected/*GemStoneAddition*/ class Broadcaster {
        private final Vector suspected_mbrs=new Vector(7);
//        BroadcastTask task=null; GemStoneAddition
//        private final Object bcast_mutex=new Object(); GemStoneAddition


//        Vector getSuspectedMembers() {
//            return suspected_mbrs;
//        }
//
        /**
         * Starts a new task, or - if already running - adds the argument to the running task.
         * @param suspect
         */
//        private void startBroadcastTask(Address suspect) {
//            synchronized(bcast_mutex) {
//                if(task == null || task.cancelled()) {
//                    task=new BroadcastTask((Vector)suspected_mbrs.clone());
//                    task.addSuspectedMember(suspect);
//                    task.run();      // run immediately the first time
//                    timer.add(task); // then every timeout milliseconds, until cancelled
//                    if(trace)
//                        log.trace("BroadcastTask started");
//                }
//                else {
//                    task.addSuspectedMember(suspect);
//                }
//            }
//        }

//        private void stopBroadcastTask() {
//            synchronized(bcast_mutex) {
//                if(task != null) {
//                    task.stop();
//                    task=null;
//                }
//            }
//        }


        // GemStoneAddition - the broadcaster is disabled in GemFire.  All
        // SUSPECT messages are sent by FD_SOCK after performing socket-connect
        // verification
        
        /** Adds a suspected member. Starts the task if not yet running */
        void addSuspectedMember(Address mbr) {
            if(mbr == null) return;
            synchronized(this) {  // GemStone - since members may be cleared, we need a sync
              if(!members.contains(mbr)) return;
            }
            synchronized(suspected_mbrs) {
                if(!suspected_mbrs.contains(mbr)) {
                    suspected_mbrs.addElement(mbr);
//                    startBroadcastTask(mbr);
                }
            }
        }

        void removeSuspectedMember(Address suspected_mbr) {
            if(suspected_mbr == null) return;
            if(log.isDebugEnabled()) log.debug("removing suspect member " + suspected_mbr);
            synchronized(suspected_mbrs) {
                suspected_mbrs.removeElement(suspected_mbr);
//                if(suspected_mbrs.size() == 0)
//                    stopBroadcastTask();
            }
        }
        
        /**
         * GemStoneAddition - test to see if member is currently suspected
         * @param mbr the address of the member in question
         * @return true if the member is under suspicion
         */
        boolean isSuspectedMember(Address mbr) {
          synchronized(suspected_mbrs) {
            return suspected_mbrs.contains(mbr);
          }
        }

        void removeAll() {
            synchronized(suspected_mbrs) {
                suspected_mbrs.removeAllElements();
//                stopBroadcastTask();
            }
        }

        /** Removes all elements from suspected_mbrs that are <em>not</em> in the new membership */
        void adjustSuspectedMembers(List new_mbrship) {
            if(new_mbrship == null || new_mbrship.size() == 0) return;
            StringBuffer sb=new StringBuffer();
            synchronized(suspected_mbrs) {
                if (log.isDebugEnabled()) sb.append("suspected_mbrs: ").append(suspected_mbrs);
                suspected_mbrs.retainAll(new_mbrship);
//                if(suspected_mbrs.size() == 0)
//                    stopBroadcastTask();
                if (log.isDebugEnabled()) sb.append(", after adjustment: ").append(suspected_mbrs);
                log.debug(sb.toString());
            }
        }
    }

/*
    private class BroadcastTask implements TimeScheduler.Task {
        boolean cancelled=false;
        private final Vector suspected_members=new Vector();


        BroadcastTask(Vector suspected_members) {
            this.suspected_members.addAll(suspected_members);
        }

        public void stop() {
            cancelled=true;
            suspected_members.clear();
            if(trace)
                log.trace("BroadcastTask stopped");
        }

        public boolean cancelled() {
            return cancelled;
        }

        public long nextInterval() {
            return FD.this.timeout;
        }

        public void run() {
            Message suspect_msg;
            FD.FdHeader hdr;

            synchronized(suspected_members) {
                if(suspected_members.size() == 0) {
                    stop();
                    if(log.isDebugEnabled()) log.debug("task done (no suspected members)");
                    return;
                }

                hdr=new FdHeader(FdHeader.SUSPECT);
                hdr.mbrs=(Vector)suspected_members.clone();
                hdr.from=local_addr;
            }
            suspect_msg=new Message();       // mcast SUSPECT to all members
            suspect_msg.putHeader(name, hdr);
            suspect_msg.isHighPriority = true;
            if(log.isDebugEnabled())
                log.debug("broadcasting SUSPECT message [suspected_mbrs=" + suspected_members + "] to group");
            passDown(new Event(Event.MSG, suspect_msg));
            if(log.isDebugEnabled()) log.debug("task done");
        }

        public void addSuspectedMember(Address suspect) {
            if(suspect != null && !suspected_members.contains(suspect)) {
                suspected_members.add(suspect);
            }
        }
    }
*/
}
