/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: UNICAST.java,v 1.47 2005/12/16 16:11:17 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Global;
import com.gemstone.org.jgroups.Header;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.ViewId;
import com.gemstone.org.jgroups.stack.AckReceiverWindow;
import com.gemstone.org.jgroups.stack.AckSenderWindow;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.BoundedList;
import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Queue;
import com.gemstone.org.jgroups.util.QueueClosedException;
import com.gemstone.org.jgroups.util.Streamable;
import com.gemstone.org.jgroups.util.TimeScheduler;
import com.gemstone.org.jgroups.util.Util;


/**
 * Reliable unicast layer. Uses acknowledgement scheme similar to TCP to provide lossless transmission
 * of unicast messages (for reliable multicast see NAKACK layer). When a message is sent to a peer for
 * the first time, we add the pair <peer_addr, Entry> to the hashtable (peer address is the key). All
 * messages sent to that peer will be added to hashtable.peer_addr.sent_msgs. When we receive a
 * message from a peer for the first time, another entry will be created and added to the hashtable
 * (unless already existing). Msgs will then be added to hashtable.peer_addr.received_msgs.<p> This
 * layer is used to reliably transmit point-to-point messages, that is, either messages sent to a
 * single receiver (vs. messages multicast to a group) or for example replies to a multicast message. The 
 * sender uses an <code>AckSenderWindow</code> which retransmits messages for which it hasn't received
 * an ACK, the receiver uses <code>AckReceiverWindow</code> which keeps track of the lowest seqno
 * received so far, and keeps messages in order.<p>
 * Messages in both AckSenderWindows and AckReceiverWindows will be removed. A message will be removed from
 * AckSenderWindow when an ACK has been received for it and messages will be removed from AckReceiverWindow
 * whenever a message is received: the new message is added and then we try to remove as many messages as
 * possible (until we stop at a gap, or there are no more messages).
 * @author Bela Ban
 */
public class UNICAST extends Protocol implements AckSenderWindow.RetransmitCommand {
    private ViewId myViewId; // GemStoneAddition
    private final Vector     members=new Vector(11);
    private final Vector     tmp_members=new Vector(11);  // GemStoneAddition - members in view currently being formed.  See CoordGmsImpl
    private final HashMap    connections=new HashMap(11);   // Object (sender or receiver) -- Entries
    private long[]           timeout={400,800,1600,3200};  // for AckSenderWindow: max time to wait for missing acks
    private Address          local_addr=null;
    private TimeScheduler    timer=null;                    // used for retransmissions (passed to AckSenderWindow)
    private volatile boolean connected; // GemStoneAddition

    // if UNICAST is used without GMS, don't consult the membership on retransmit() if use_gms=false
    // default is true
    private boolean          use_gms=true;

    /** A list of members who left, used to determine when to prevent sending messages to left mbrs */
    private final BoundedList previous_members=new BoundedList(50);

    private final static String name="UNICAST";
    public static final long DEFAULT_FIRST_SEQNO=1;

    private long num_msgs_sent=0, num_msgs_received=0, num_bytes_sent=0, num_bytes_received=0;
    private long num_acks_sent=0, num_acks_received=0, num_xmit_requests_received=0;


    /** All protocol names have to be unique ! */
    @Override // GemStoneAddition
    public String  getName() {return name;}
    
    private AckSender ackSender; // GemStoneAddition - acksender thread
    
    /**
     * GemStoneAddition - track current number of received but undispatched messages
     */
    private final AtomicLong numReceivedMsgs = new AtomicLong();
    /**
     * GemStoneAddition - track current number of unacked sent messages with
     * normal priority
     */
    private final AtomicLong numSentMsgs = new AtomicLong();
    /**
     * GemStoneAddition - track current number of unacked high priority messages
     */
    private final AtomicLong numSentHighPrioMsgs = new AtomicLong();
    
    /**
     * GemStoneAddition - sync delivery to prevent race between ucast receiver thread
     * and ucast message handler thread delivering messages out of order
     */
    private final Object deliverySync = new Object();
    
    private long max_xmit_burst=0; // GemStoneAddition
    
    
    // start GemStoneAddition
    @Override // GemStoneAddition
    public int getProtocolEnum() {
      return com.gemstone.org.jgroups.stack.Protocol.enumUNICAST;
    }
    // end GemStone addition



    public String getLocalAddress() {return local_addr != null? local_addr.toString() : "null";}
    public String getMembers() {return members != null? members.toString() : "[]";}
    public String printConnections() {
        StringBuffer sb=new StringBuffer();
        Map.Entry entry;
        for(Iterator it=connections.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }


    public long getNumMessagesSent() {
        return num_msgs_sent;
    }

    public long getNumMessagesReceived() {
        return num_msgs_received;
    }

    public long getNumBytesSent() {
        return num_bytes_sent;
    }

    public long getNumBytesReceived() {
        return num_bytes_received;
    }

    public long getNumAcksSent() {
        return num_acks_sent;
    }

    public long getNumAcksReceived() {
        return num_acks_received;
    }

    public long getNumberOfRetransmitRequestsReceived() {
        return num_xmit_requests_received;
    }

    /** The number of messages in all Entry.sent_msgs tables (haven't received an ACK yet) */
    public int getNumberOfUnackedMessages() {
        int num=0;
        Entry entry;
        synchronized(connections) {
            for(Iterator it=connections.values().iterator(); it.hasNext();) {
                entry=(Entry)it.next();
                if(entry.sent_msgs != null)
                num+=entry.sent_msgs.size();
                num+=entry.sent_high_prio_msgs.size();
            }
        }
        return num;
    }

    @Override // GemStoneAddition
    public void resetStats() {
        num_msgs_sent=num_msgs_received=num_bytes_sent=num_bytes_received=num_acks_sent=num_acks_received=0;
        num_xmit_requests_received=0;
    }

    @Override // GemStoneAddition
    public Map dumpStats() {
        Map m=new HashMap();
        m.put("num_msgs_sent", Long.valueOf(num_msgs_sent));
        m.put("num_msgs_received", Long.valueOf(num_msgs_received));
        m.put("num_bytes_sent", Long.valueOf(num_bytes_sent));
        m.put("num_bytes_received", Long.valueOf(num_bytes_received));
        m.put("num_acks_sent", Long.valueOf(num_acks_sent));
        m.put("num_acks_received", Long.valueOf(num_acks_received));
        m.put("num_xmit_requests_received", Long.valueOf(num_xmit_requests_received));
        return m;
    }

    @Override // GemStoneAddition
    public boolean setProperties(Properties props) {
        String     str;
        long[]     tmp;

        super.setProperties(props);
        str=props.getProperty("timeout");
        if(str != null) {
        tmp=Util.parseCommaDelimitedLongs(str);
        if(tmp != null && tmp.length > 0)
        timeout=tmp;
            props.remove("timeout");
        }

        str=props.getProperty("window_size");
        if(str != null) {
            props.remove("window_size");
            log.error(ExternalStrings.UNICAST_WINDOW_SIZE_IS_DEPRECATED_AND_WILL_BE_IGNORED);
        }

        str=props.getProperty("min_threshold");
        if(str != null) {
            props.remove("min_threshold");
            log.error(ExternalStrings.UNICAST_MIN_THRESHOLD_IS_DEPRECATED_AND_WILL_BE_IGNORED);
        }

        str=props.getProperty("use_gms");
        if(str != null) {
            use_gms=Boolean.valueOf(str).booleanValue();
            props.remove("use_gms");
        }

        // GemStoneAddition - limit size of retransmission bursts
        str=props.getProperty("max_xmit_burst");
        if (str != null) {
          max_xmit_burst = Long.parseLong(str);
          props.remove("max_xmit_burst");
        }

        if(props.size() > 0) {
            log.error(ExternalStrings.UNICAST_UNICASTSETPROPERTIES_THESE_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);

            return false;
        }
        return true;
    }

    @Override // GemStoneAddition
    public void start() throws Exception {
      removeAllConnections(); // GemStoneAddition
        timer=stack != null ? stack.timer : null;
        if(timer == null)
            throw new Exception("timer is null");
        // GemStoneAddition ackSender thread
        if (Boolean.getBoolean("p2p.ackSenderThread")) {
          if (ackSender == null)
            ackSender = new AckSender();
          ackSender.start();
        }
    }

    @Override // GemStoneAddition
    public void stop() {
        // GemStoneAddition ackSender thread
        if (ackSender != null)
          ackSender.stop();
//        removeAllConnections();  GemStoneAddition - due to many shutdown race 
        // conditions in JGroups we don't do this.  Instead, clear connections 
        // if the channel is restarted
    }


    @Override // GemStoneAddition
    public void up(Event evt) {
        Message        msg;
        Address        dst, src;
        UnicastHeader  hdr;

        switch(evt.getType()) {

        case Event.MSG:
            msg=(Message)evt.getArg();
            dst=msg.getDest();


            if(dst == null || dst.isMulticastAddress())  // only handle unicast messages
                break;  // pass up

            // changed from removeHeader(): we cannot remove the header because if we do loopback=true at the
            // transport level, we will not have the header on retransmit ! (bela Aug 22 2006)
            hdr=(UnicastHeader)msg.getHeader(name);
            if(hdr == null)
                break;
            src=msg.getSrc();
            switch(hdr.type) {
            case UnicastHeader.DATA:      // received regular message
                handleDataReceived(src, hdr.seqno, msg);
                // GemStoneAddition - acks are sent in handleDataReceived so they can be sent earlier
                // than after the full dispatch has occurred.  That can take too long and cause
                // false retransmissions
                //sendAck(src, hdr.seqno); // only send an ACK if added to the received_msgs table (bela Aug 2006)
                return; // we pass the deliverable message up in handleDataReceived()
            case UnicastHeader.ACK:  // received ACK for previously sent message
                handleAckReceived(src, hdr.seqno, msg.timeStamp, false/* GemStoneAddition*/);
                break;
            case UnicastHeader.HIGH_PRIO_ACK:  // GemStoneAddition - high priority messages
              handleAckReceived(src, hdr.seqno, msg.timeStamp, true/* GemStoneAddition*/);
              break;
            default:
                log.error(ExternalStrings.UNICAST_UNICASTHEADER_TYPE__0__NOT_KNOWN_, hdr.type);
                break;
            }
            return;

        case Event.SET_LOCAL_ADDRESS:
            local_addr=(Address)evt.getArg();
            break;
        }

        passUp(evt);   // Pass up to the layer above us
    }


    public static final String BYPASS_UNICAST = "NO_UCAST";



    @Override // GemStoneAddition
    public void down(Event evt) {
        switch (evt.getType()) {

        case Event.CONNECT_OK:
          // GemStoneAddition - we need to know when we've actually connected the jgroups channel
          connected = true;
          break;
          
            case Event.MSG: // Add UnicastHeader, add to AckSenderWindow and pass down
                Message msg = (Message) evt.getArg();
                // GemStoneAddition - bypass UNICAST for sending this message
                if (msg.getHeader(BYPASS_UNICAST) != null) {
                  break;
                }

                Object dst = msg.getDest();


                /* only handle unicast messages */
                if (dst == null || ((Address) dst).isMulticastAddress()) {
                    break;
                }

                if(previous_members.contains(dst)) {
                    if(trace)
                        log.trace("discarding message to " + dst + " as this member left the group,");// +
//                                " previous_members=" + previous_members);
                    return;
                }

                Address addr = (Address)dst;

                Entry entry;
                synchronized(connections) {
                    entry=(Entry)connections.get(addr);
                    if(entry == null) {
                        entry=new Entry(addr);
                        connections.put(addr, entry);
                        if(trace)
                            log.trace(local_addr + ": created new UNICAST connection for recipient " + dst);
                    }
                }

                Message tmp; // GemStoneAddition - send outside of synch
                synchronized(entry) { // threads will only sync if they access the same entry
                  long seqno=entry.sent_msgs_seqno;
                  UnicastHeader hdr=new UnicastHeader(UnicastHeader.DATA, seqno);
                  if(entry.sent_msgs == null) { // first msg to peer 'dst'
                    entry.sent_msgs=new AckSenderWindow(this, timeout, timer); // use the protocol stack's timer
                    entry.sent_high_prio_msgs = new AckSenderWindow(this, timeout, timer);
                  }
                  msg.putHeader(name, hdr);
                  //if(trace)
                  //    log.trace(new StringBuffer().append("UNICAST: ").append(local_addr).append(" --> DATA(").append(dst).append(": #").
                  //            append(seqno));

                  entry.sent_msgs_seqno++;
                  tmp=Global.copy? msg.copy() : msg;

                  // GemStoneAddition - high priority distribution message support
                  if (msg.isHighPriority) {
                    entry.sent_high_prio_msgs.add(seqno, tmp);
                    entry.numSentHighPrioMsgs++;
                    long hm = this.numSentHighPrioMsgs.incrementAndGet();
                    entry.sentMsgsBytes += tmp.size();
                    stack.gfPeerFunctions.setJgUNICASTsentHighPriorityMessagesSize(hm);
                  }
                  else {
                    entry.sent_msgs.add(seqno, tmp);  // add *including* UnicastHeader, adds to retransmitter
                    entry.numSentMsgs++;
                    entry.sentMsgsBytes += tmp.size();
                    long sm = this.numSentMsgs.incrementAndGet();
                    stack.gfPeerFunctions.setJgUNICASTsentMessagesSize(sm);
                  }
                  num_msgs_sent++;
                  num_bytes_sent+=msg.getLength();
                }
                passDown(new Event(Event.MSG, tmp));
                msg=null;
                return; // AckSenderWindow will send message for us

            case Event.VIEW_CHANGE:  // remove connections to peers that are not members anymore !
                View view = (View)evt.getArg();
                Vector new_members=view.getMembers();
                Vector left_members;
                synchronized(members) {
                    left_members=Util.determineLeftMembers(members, new_members);
                    members.clear();
                    if(new_members != null)
                        members.addAll(new_members);
                    // GemStoneAddition - bug #42006 don't retain valid member addresses in previous_mbrs
                    for (Iterator it=members.iterator(); it.hasNext(); ) {
                      Object mbr = it.next();
                      previous_members.removeElement(mbr);
                    }
                    this.myViewId = view.getVid();  // GemStoneAddition - retain the view ID
                }
                // GemStoneAddition - clean up the connection table's addresses
                // see bug #46438
                synchronized (connections) {
                  for (Iterator it=connections.entrySet().iterator(); it.hasNext();) {
                    Map.Entry mapEntry = (Map.Entry)it.next();
                    IpAddress iaddr = (IpAddress)mapEntry.getKey();
                    if (iaddr.getBirthViewId() < 0) {
                      Address memberId = view.getMember(iaddr);
                      if (memberId != null
                          &&  memberId != iaddr) {  // note: identity check, not equality here.
                                                   //       getMember can return the argument
                        if (log.isInfoEnabled()) {
                          log.info("establishing UNICAST view ID of " + iaddr + " with that of " + memberId);
                        }
                        iaddr.setBirthViewId(memberId.getBirthViewId());
                        Entry e = (Entry)mapEntry.getValue();
                        e.memberId = memberId;
                      }
                    }
                  }
                }

                // Remove all connections for members that left between the current view and the new view
                // See DESIGN for details
                boolean rc;
                if(use_gms && left_members.size() > 0) {
                    Object mbr;
                    for(int i=0; i < left_members.size(); i++) {
                        mbr=left_members.elementAt(i);
                        rc=removeConnection(mbr);
                        if(rc && trace)
                            log.trace("removed " + mbr + " from connection table, member(s) " + left_members + " left");
                    }
                }
                break;

            case Event.TMP_VIEW: // GemStoneAddition - record this to allow retransmits to joining members
              synchronized(members) { // use same sync as for "members" manipulation
                tmp_members.clear();
                tmp_members.addAll(((View)evt.getArg()).getMembers());
              }
              break;
              
            case Event.ENABLE_UNICASTS_TO:
                Object member=evt.getArg();
                previous_members.removeElement(member);
                if(trace)
                    log.trace("removing " + member + " from previous_members as result of ENABLE_UNICAST_TO event, " +
                            "previous_members=" + previous_members);
                break;

            // GemStoneAddition - if a member fails to connect (reused addr or
            // authorization failure) then we need to put it back in previous_mbrs
            // and remove its connection
            case Event.DISABLE_UNICASTS_TO:
              member=evt.getArg();
              if(trace)
                log.trace("removing connection for " + member + " as result of DISABLE_UNICAST_TO event, " +
                        "previous_members=" + previous_members);
              removeConnection(member);
              break;
        
        }

        passDown(evt);          // Pass on to the layer below us
    }
    
    
    private boolean removeConnection(Object mbr) {
      return removeConnection(mbr, true);
    }


    /** Removes and resets from connection table (which is already locked). Returns true if member was found, otherwise false */
    private boolean removeConnection(Object mbr, boolean addToPreviousMembers) {
        Entry entry;

        synchronized(connections) {
            entry=(Entry)connections.remove(mbr);
            if(addToPreviousMembers  &&  !previous_members.contains(mbr))
                previous_members.add(mbr);
        }
        if(entry != null) {
          synchronized(entry) {
            long rm = this.numReceivedMsgs.addAndGet(-entry.numReceivedMsgs);
            long sm = this.numSentMsgs.addAndGet(-entry.numSentMsgs);
            long hm = this.numSentHighPrioMsgs.addAndGet(-entry.numSentHighPrioMsgs);
            stack.gfPeerFunctions.setJgUNICASTreceivedMessagesSize(rm);
            stack.gfPeerFunctions.setJgUNICASTsentMessagesSize(sm);
            stack.gfPeerFunctions.setJgUNICASTsentHighPriorityMessagesSize(hm);
          }
            entry.reset();
            if(trace)
                log.trace(local_addr + ": removed connection for dst " + mbr);
            return true;
        }
        else
            return false;
    }


    private void removeAllConnections() {
        Entry entry;

        synchronized(connections) {
            for(Iterator it=connections.values().iterator(); it.hasNext();) {
                entry=(Entry)it.next();
                entry.reset();
            }
            connections.clear();
        }
    }



    /** Called by AckSenderWindow to resend messages for which no ACK has been received yet */
    public void retransmit(long seqno, Message msg) {
        //Object  dst=msg.getDest();

        // bela Dec 23 2002:
        // this will remove a member on a MERGE request, e.g. A and B merge: when A sends the unicast
        // request to B and there's a retransmit(), B will be removed !

        //          if(use_gms && !members.contains(dst) && !prev_members.contains(dst)) {
        //
        //                  if(warn) log.warn("UNICAST.retransmit()", "seqno=" + seqno + ":  dest " + dst +
        //                             " is not member any longer; removing entry !");

        //              synchronized(connections) {
        //                  removeConnection(dst);
        //              }
        //              return;
        //          }

//        if(trace)
//            log.trace("[" + local_addr + "] --> XMIT(" + dst + ": #" + seqno + ')');
      
      if (connected) {
        // GemStoneAddition - don't send to non-members
        synchronized(members) {
          if (!members.contains(msg.getDest()) && !tmp_members.contains(msg.getDest())) {
            IpAddress mbr = (IpAddress)msg.getDest();
            // if it's an old member that's no longer there stop retransmissions to it
            if (myViewId != null && mbr.getBirthViewId() > 0 && mbr.getBirthViewId() <= myViewId.getId()) {
              if (trace) {
                log.trace("stopping retransmission of message for non member: " + msg);
              }
              throw new IllegalStateException();
            }
          }
        }
      }
      
      msg.bundleable = false; // don't bundle retransmissions

        if(Global.copy)
            passDown(new Event(Event.MSG, msg.copy()));
        else
            passDown(new Event(Event.MSG, msg));
        num_xmit_requests_received++;
        stack.gfPeerFunctions.incUcastRetransmits(); // GemStoneAddition
    }


    public long getMaxRetransmissionBurst() {
      return this.max_xmit_burst;
    }





    /**
     * Check whether the hashtable contains an entry e for <code>sender</code> (create if not). If
     * e.received_msgs is null and <code>first</code> is true: create a new AckReceiverWindow(seqno) and
     * add message. Set e.received_msgs to the new window. Else just add the message.
     */
    private void handleDataReceived(Address sender, long seqno, Message msg) {
        // GemStoneAddition - this trace is redundant.  We see the message in TP
        //if(trace)
        //    log.trace(new StringBuffer().append(local_addr).append(" <-- DATA(").append(sender).append(": #").append(seqno));


        if (previous_members.contains(sender)) {
            // GemStoneAddition - we don't want to see messages from departed members
            if (seqno > DEFAULT_FIRST_SEQNO) {
              if (trace)
                log.trace("discarding message " + seqno + " from previous member " + sender);
              return;
            }
            if(trace)
                log.trace("removed " + sender + " from previous_members as we received a message from it");
            previous_members.removeElement(sender);
        }
        
        if (debug39744 && msg.isJoinResponse) {
          log.getLogWriter().info(ExternalStrings.DEBUG, "received " + msg + " from " + sender);
        }

        // this causes the udp ucast thread to hang until startup times out
        // because startup holds the view lock
//        if (this.stack.jgmm.isShunnedMember((IpAddress)sender)) {
//          if (trace) {
//            log.trace("discarding message from shunned member: " + msg);
//          }
//          return;
//        }
      

        // GemStoneAddition - we're accepting the message so send an ack now 
        sendAck(msg.getSrc(), seqno, msg.isHighPriority); // GemStoneAddition - high priority messages

        Entry    entry;
        synchronized(connections) {
          entry=(Entry)connections.get(sender);
          if (trace)
            log.trace("found UNICAST entry for " + sender + ": " + entry);
//          if (sender.getBirthViewId() < 0 && seqno == DEFAULT_FIRST_SEQNO) {
//            while (entry != null && entry.memberId.getBirthViewId() >= 0) {
//              // an address has been reused - remove the old member from the connection table
//              connections.remove(sender);
//              if(trace)
//                log.trace(local_addr + ": removed UNICAST connection for old sender " + entry.memberId);
//              entry = (Entry)connections.get(sender);
//            }
//          }
          if(entry == null) {
            entry=new Entry(sender);
            connections.put(sender, entry);
            if(trace)
              log.trace(local_addr + ": created new UNICAST connection for sender " + sender);
          }
          if(entry.received_msgs == null) {
            entry.received_msgs=new AckReceiverWindow(DEFAULT_FIRST_SEQNO);
          }
        }

        // GemStoneAddition - fix for bug #39457, similar to change in NAKACK
        synchronized(deliverySync) {
          boolean alreadyAdded = !entry.received_msgs.add(seqno, msg); // entry.received_msgs is guaranteed to be non-null if we get here
          num_msgs_received++;
          num_bytes_received+=msg.getLength();
          if (!alreadyAdded) { // GemStoneAddition - don't reprocess if we already had the message

            // GemStoneAddition - dispatch high prio messages immediately
            if (msg.isHighPriority) {
              if (stack.enableClockStats && msg.timeStamp > 0) { // GemStoneAddition - statistics
                stack.gfPeerFunctions.incJgUNICASTdataReceived(nanoTime()-msg.timeStamp);
              }
              if (TP.VERBOSE) {
                try {
                  log.getLogWriter().info(ExternalStrings.DEBUG, "dispatching message with payload " + msg.getObject());
                } catch (Exception e) {
                  log.getLogWriter().info(ExternalStrings.DEBUG, "dispatching message (payload not deserializable)", e);
                }
              }
              passUp(new Event(Event.MSG, msg));
            }
            else {
              if (TP.VERBOSE) {
                if (!entry.received_msgs.isNextToRemove(seqno)) {
                  log.getLogWriter().info(
                      ExternalStrings.ONE_ARG,
                      "waiting for unicast msg with seqno "
                      + entry.received_msgs.nextToRemove());
                }
              }
              synchronized(entry) {
                entry.numReceivedMsgs++;
                long rm = this.numReceivedMsgs.incrementAndGet();
                if (stack.gfPeerFunctions != null) {
                  stack.gfPeerFunctions.setJgUNICASTreceivedMessagesSize(rm);
                }
              }
            }

            // Try to remove (from the AckReceiverWindow) as many messages as possible as pass them up
            Message  m;
            while((m=entry.received_msgs.remove()) != null) {
              if (!m.isHighPriority) { // GemStoneAddition - already dispatched
                if (stack.enableClockStats && m.timeStamp > 0)  { // GemStoneAddition - statistics
                  stack.gfPeerFunctions.incJgUNICASTdataReceived(nanoTime()-m.timeStamp);
                }
//                if (trace) log.trace("UNICAST is dispatching " + m + " with payload " + m.toStringAsObject());
                if (TP.VERBOSE) {
                  try {
                    log.getLogWriter().info(ExternalStrings.DEBUG, "dispatching message " + m + " with payload " + msg.getObject());
                  } catch (Exception e) {
                    log.getLogWriter().info(ExternalStrings.DEBUG, "dispatching message " + m + " (payload not deserializable)", e);
                  }
                }
                passUp(new Event(Event.MSG, m));
                synchronized(entry) {
                  entry.numReceivedMsgs--;
                  long rm = this.numReceivedMsgs.decrementAndGet();
                  stack.gfPeerFunctions.setJgUNICASTreceivedMessagesSize(rm);
                }
              }
            }
          }
        }
    }


    boolean debug39744 = Boolean.getBoolean("debug39744");

    /** Add the ACK to hashtable.sender.sent_msgs */
    private void handleAckReceived(Object sender, long seqno,
      long timeStamp /* GemStoneAddition */, boolean highPriority /*GemStoneAddition*/) {
        Entry           entry;
        synchronized(connections) {
            entry=(Entry)connections.get(sender);
        }
        if (debug39744) {
          log.getLogWriter().info(ExternalStrings.DEBUG, "ack received from " + sender + " for " + seqno);
        }
        if(entry == null || (highPriority && entry.sent_high_prio_msgs == null) || (!highPriority && entry.sent_msgs == null)) {
            if (trace) log.trace("No entry found for unicast ack from " + sender);
            return;
        }
        if (highPriority) {
          long size = entry.sent_high_prio_msgs.ack(seqno);
          if (size >= 0) { // GemStoneAddition - high priority message support
            entry.numSentHighPrioMsgs--;
            long hm = this.numSentHighPrioMsgs.decrementAndGet();
            entry.sentMsgsBytes -= size;
            stack.gfPeerFunctions.setJgUNICASTsentHighPriorityMessagesSize(hm);
          }
        }
        else {
          long size = entry.sent_msgs.ack(seqno);
          if (size >= 0) {
            synchronized(entry) {
              entry.numSentMsgs--;
              entry.sentMsgsBytes -= size;
              long sm = this.numSentMsgs.decrementAndGet();
              stack.gfPeerFunctions.setJgUNICASTsentMessagesSize(sm);
            }
          }
        }
        num_acks_received++;
    }



    private void sendAck(Address dst, long seqno, boolean highPriority) {
        Message ack=new Message(dst, null, null);
        ack.putHeader(name, new UnicastHeader(
            highPriority?UnicastHeader.HIGH_PRIO_ACK:UnicastHeader.ACK, // GemStoneAddition
            seqno));
        ack.isHighPriority = true;
        //if(trace)
        //    log.trace(new StringBuffer().append(local_addr).append(" --> ACK(").append(dst).
        //              append(": #").append(seqno).append(')'));
        //GemStoneAddition - ack sender thread
        Event e = new Event(Event.MSG, ack);
        if (ackSender == null)
          passDown(e);
        else {
          try {
            ackSender.ackQueue.add(e);
          }
          catch (QueueClosedException qe) {
            passDown(e);
          }
        }
        num_acks_sent++;
    }



    public void dumpAckSenderWindows() {
      synchronized(connections) {
        log.getLogWriter().info(ExternalStrings.DEBUG, "dump of UNICAST sender windows:");
        for (Iterator it = connections.entrySet().iterator(); it.hasNext(); ) {
          Map.Entry entry = (Map.Entry)it.next();
          log.getLogWriter().info(ExternalStrings.DEBUG, "receiver=" + entry.getKey() + " entry=" + entry.getValue());
        }
//        log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "dump of UNICAST old sender windows:");
//        for (Iterator it = previousconnections.entrySet().iterator(); it.hasNext(); ) {
//          Map.Entry entry = (Map.Entry)it.next();
//          log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "receiver=" + entry.getKey() + " entry=" + entry.getValue());
//        }
      }
    }


    public void dumpAckSenderWindow(Address addr) {
      Entry entry = null;
      synchronized(connections) {
        entry = (Entry)connections.get(addr);
      }
      if (entry == null) {
        log.getLogWriter().info(ExternalStrings.DEBUG, "UNICAST: no entry found for " + addr);
      } else {
        synchronized(entry) {
          log.getLogWriter().info(ExternalStrings.DEBUG, "UNICAST: high priority ack sender window for " + addr + " is " + entry.sent_high_prio_msgs);
        }
      }
    }

    public static class UnicastHeader extends Header implements Streamable {
        public static final byte DATA=0;
        public static final byte ACK=1;
        public static final byte HIGH_PRIO_ACK=2; // GemStoneAddition - high priority message support

        byte    type=DATA;
        long    seqno=0;

        static final long serialized_size=Global.BYTE_SIZE + Global.LONG_SIZE;


        public UnicastHeader() {} // used for externalization

        public UnicastHeader(byte type, long seqno) {
            this.type=type;
            this.seqno=seqno;
        }

        @Override // GemStoneAddition
        public String toString() {
            return "[UNICAST: " + type2Str(type) + ", seqno=" + seqno + ']';
        }

        public static String type2Str(byte t) {
            switch(t) {
                case DATA: return "DATA";
                case ACK: return "ACK";
                case HIGH_PRIO_ACK: return "HIGH_PRIO_ACK";
                default: return "<unknown>";
            }
        }

        @Override // GemStoneAddition
        public long size(short version) {
            return serialized_size;
        }


        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(type);
            out.writeLong(seqno);
        }



        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readByte();
            seqno=in.readLong();
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
            out.writeLong(seqno);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readByte();
            seqno=in.readLong();
        }
    }

    protected/*GemStoneAddition*/ static final class Entry {
        Address memberId; // GemStoneAddition
        AckReceiverWindow  received_msgs=null;  // stores all msgs rcvd by a certain peer in seqno-order
        AckSenderWindow    sent_msgs=null;      // stores (and retransmits) msgs sent by us to a certain peer
        AckSenderWindow    sent_high_prio_msgs; // GemStoneAddition
        long     numReceivedMsgs; // GemStoneAddition
        long     numSentMsgs; // GemStoneAddition
        long     numSentHighPrioMsgs; // GemStoneAddition
        long     sentMsgsBytes; // GemStoneAddition
        long               sent_msgs_seqno=DEFAULT_FIRST_SEQNO;   // seqno for msgs sent by us

        Entry(Address mbr) {
          this.memberId = mbr;
        }
        
        void reset() {
            if(sent_msgs != null)
                sent_msgs.reset();
            if (sent_high_prio_msgs != null)
              sent_high_prio_msgs.reset();
            if(received_msgs != null)
                received_msgs.reset();
            numReceivedMsgs = numSentMsgs = numSentHighPrioMsgs = 0; // GemStoneAddition
        }


        @Override // GemStoneAddition
        public String toString() {
            StringBuffer sb=new StringBuffer();
            sb.append(this.memberId);
            if(sent_msgs != null)
                sb.append("; sent_msgs=" + sent_msgs);
            if(sent_high_prio_msgs != null)
              sb.append("; sent_high_prio_msgs=" + sent_msgs);
            if(received_msgs != null)
                sb.append("; received_msgs=" + received_msgs);
            return sb.toString();
        }
    }
    
    class AckSender implements Runnable {
      public com.gemstone.org.jgroups.util.Queue ackQueue = new com.gemstone.org.jgroups.util.Queue();
      public Thread t; // GemStoneAddition synchronize via this
      public AckSender() {
      }
      /** creates and starts a thread for this sender */
      public void start() {
        synchronized (this) { // GemStoneAddition
        t = new Thread(GemFireTracer.GROUP, this, "AckSender");
        t.setDaemon(true);
        ackQueue = new Queue();
        t.start();
        }
      }
      /** closes the sender queue and flags the sender to stop */
      public void stop() {
        ackQueue.close(true);
        // GemStoneAddition:
        synchronized (this) {
          if (t != null) {
            t.interrupt();
          }
          t = null;
        }
      }
      public void run() {
        for (;;) { // GemStoneAddition -- remove coding anti-pattern
          if (ackQueue.closed()) break; // GemStoneAddition
          if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
          try {
            Event e = (Event)ackQueue.remove();
            passDown(e);
          }
          catch (InterruptedException ie) {
            if (log.isTraceEnabled()) log.trace("ack sender thread terminated by interrupt");
            Thread.currentThread().interrupt();
            return; // exit loop and thread
          }
          catch (QueueClosedException ce) {
            return;
          }
          catch (VirtualMachineError err) { // GemStoneAddition
            // If this ever returns, rethrow the error.  We're poisoned
            // now, so don't let this thread continue.
            throw err;
          }
          catch (Throwable ex) {
            if (log.isErrorEnabled())
              log.error(ExternalStrings.UNICAST_ERROR_PROCESSING_OUTGOING_ACK_EVENT, ex);
          }
          if (trace) log.trace("ack sender thread terminating");
        }
      }
    }


}
