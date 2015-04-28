/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: NAKACK.java,v 1.61 2005/12/16 16:10:26 belaban Exp $

package com.gemstone.org.jgroups.protocols.pbcast;


import com.gemstone.org.jgroups.*;
import com.gemstone.org.jgroups.protocols.pbcast.GMS.GmsHeader;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.stack.NakReceiverWindow;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.stack.Retransmitter;
import com.gemstone.org.jgroups.util.*;
import com.gemstone.org.jgroups.util.TimeScheduler.Task;

import java.util.*;
import java.util.List;
import java.io.*;


/**
 * Negative AcKnowledgement layer (NAKs). Messages are assigned a monotonically increasing sequence number (seqno).
 * Receivers deliver messages ordered according to seqno and request retransmission of missing messages. Retransmitted
 * messages are bundled into bigger ones, e.g. when getting an xmit request for messages 1-10, instead of sending 10
 * unicast messages, we bundle all 10 messages into 1 and send it. However, since this protocol typically sits below
 * FRAG, we cannot count on FRAG to fragement/defragment the (possibly) large message into smaller ones. Therefore we
 * only bundle messages up to max_xmit_size bytes to prevent too large messages. For example, if the bundled message
 * size was a total of 34000 bytes, and max_xmit_size=16000, we'd send 3 messages: 2 16K and a 2K message. <em>Note that
 * max_xmit_size should be the same value as FRAG.frag_size (or smaller).</em><br/> Retransmit requests are always sent
 * to the sender. If the sender dies, and not everyone has received its messages, they will be lost. In the future, this
 * may be changed to have receivers store all messages, so that retransmit requests can be answered by any member.
 * Trivial to implement, but not done yet. For most apps, the default retransmit properties are sufficient, if not use
 * vsync.
 *
 * @author Bela Ban
 */
public class NAKACK extends Protocol implements Retransmitter.RetransmitCommand, NakReceiverWindow.Listener {
    private long[]  retransmit_timeout={600, 1200, 2400, 4800}; // time(s) to wait before requesting retransmission
    private volatile/*GemStoneAddition*/ boolean is_server=false;
    private Address local_addr=null;
    private final Vector  members=new Vector(11);
    private ViewId viewId; // GemStoneAddition
    private long    seqno=0;                                   // current message sequence number (starts with 0)
    private long    max_xmit_size=8192;                        // max size of a retransmit message (otherwise send multiple)
    private int     gc_lag=20;                                 // number of msgs garbage collection lags behind

    private long max_xmit_burst=0; // GemStoneAddition
    /**
     * Retransmit messages using multicast rather than unicast. This has the advantage that, if many receivers lost a
     * message, the sender only retransmits once.
     */
    private boolean use_mcast_xmit=false;

    /**
     * Ask a random member for retransmission of a missing message. If set to true, discard_delivered_msgs will be
     * set to false
     */
    private boolean xmit_from_random_member=false;


    /**
     * Messages that have been received in order are sent up the stack (= delivered to the application). Delivered
     * messages are removed from NakReceiverWindow.received_msgs and moved to NakReceiverWindow.delivered_msgs, where
     * they are later garbage collected (by STABLE). Since we do retransmits only from sent messages, never
     * received or delivered messages, we can turn the moving to delivered_msgs off, so we don't keep the message
     * around, and don't need to wait for garbage collection to remove them.
     */
    private boolean discard_delivered_msgs=false;

    /** If value is > 0, the retransmit buffer is bounded: only the max_xmit_buf_size latest messages are kept,
     * older ones are discarded when the buffer size is exceeded. A value <= 0 means unbounded buffers
     */
    private int max_xmit_buf_size=0;


    /**
     * Hashtable key is Address, value is NakReceiverWindow. Stores received messages (keyed by sender). Note that this is no long term
     * storage; messages are just stored until they can be delivered (ie., until the correct FIFO order is established)
     */
    private final HashMap received_msgs=new HashMap(11);
    
    private long received_msgs_size = 0; // statistic

   /** TreeMap key is Long, value is Message. Map of messages sent by me (keyed and sorted on sequence number) */
    private final TreeMap sent_msgs=new TreeMap();
    
    private boolean leaving=false;
    private TimeScheduler timer=null;
    private static final String name="NAKACK";

    private long xmit_reqs_received;
    private long xmit_reqs_sent;
    private long xmit_rsps_received;
    private long xmit_rsps_sent;
    private long missing_msgs_received;

    /** Captures stats on XMIT_REQS, XMIT_RSPS per sender */
    private HashMap sent=new HashMap();

    /** Captures stats on XMIT_REQS, XMIT_RSPS per receiver */
    private HashMap received=new HashMap();

    private int stats_list_size=20;

    /** BoundedList values are XmitRequest. Keeps track of the last stats_list_size XMIT requests */
    private BoundedList receive_history;

    /** BoundedList values are MissingMessage. Keeps track of the last stats_list_size missing messages received */
    private BoundedList send_history;

    // GemStoneAddition
    long max_sent_msgs_size = 100000; // maximum number of sent messages



    public NAKACK() {
    }


    @Override // GemStoneAddition  
    public String getName() {
        return name;
    }

    // start GemStoneAddition
    @Override // GemStoneAddition  
    public int getProtocolEnum() {
      return com.gemstone.org.jgroups.stack.Protocol.enumNAKACK;
    }
    // end GemStone addition


    public long getXmitRequestsReceived() {return xmit_reqs_received;}
    public long getXmitRequestsSent() {return xmit_reqs_sent;}
    public long getXmitResponsesReceived() {return xmit_rsps_received;}
    public long getXmitResponsesSent() {return xmit_rsps_sent;}
    public long getMissingMessagesReceived() {return missing_msgs_received;}

    public int getPendingRetransmissionRequests() {
        // TODO just return received_msgs_size?
        int num=0;
        NakReceiverWindow win;
        synchronized(received_msgs) {
            for(Iterator it=received_msgs.values().iterator(); it.hasNext();) {
                win=(NakReceiverWindow)it.next();
                num+=win.size();
            }
       }
         return num;
    }

    public int getSentTableSize() {
        return sent_msgs.size();
    }

    public int getReceivedTableSize() {
        // TODO: just return received_msgs_size?
        // TODO: why is this sum not calculated in a synchronized block?
        int ret=0;
        NakReceiverWindow win;
        Set s=new LinkedHashSet(received_msgs.values());
        for(Iterator it=s.iterator(); it.hasNext();) {
            win=(NakReceiverWindow)it.next();
            ret+=win.size();
        }
        return ret;
    }

    @Override // GemStoneAddition  
    public void resetStats() {
        xmit_reqs_received=xmit_reqs_sent=xmit_rsps_received=xmit_rsps_sent=missing_msgs_received=0;
        sent.clear();
        received.clear();
        if(receive_history !=null)
            receive_history.removeAll();
        if(send_history != null)
            send_history.removeAll();
    }

    @Override // GemStoneAddition  
    public void init() throws Exception {
        if(stats) {
            send_history=new BoundedList(stats_list_size);
            receive_history=new BoundedList(stats_list_size);
        }
    }


    public int getGcLag() {
        return gc_lag;
    }

    public void setGcLag(int gc_lag) {
        this.gc_lag=gc_lag;
    }

    public boolean isUseMcastXmit() {
        return use_mcast_xmit;
    }

    public void setUseMcastXmit(boolean use_mcast_xmit) {
        this.use_mcast_xmit=use_mcast_xmit;
    }

    public boolean isXmitFromRandomMember() {
        return xmit_from_random_member;
    }

    public void setXmitFromRandomMember(boolean xmit_from_random_member) {
        this.xmit_from_random_member=xmit_from_random_member;
    }

    public boolean isDiscardDeliveredMsgs() {
        return discard_delivered_msgs;
    }

    public void setDiscardDeliveredMsgs(boolean discard_delivered_msgs) {
        this.discard_delivered_msgs=discard_delivered_msgs;
    }

    public int getMaxXmitBufSize() {
        return max_xmit_buf_size;
    }

    public void setMaxXmitBufSize(int max_xmit_buf_size) {
        this.max_xmit_buf_size=max_xmit_buf_size;
    }

    public long getMaxXmitSize() {
        return max_xmit_size;
    }

    public void setMaxXmitSize(long max_xmit_size) {
        this.max_xmit_size=max_xmit_size;
    }

    @Override // GemStoneAddition  
    public boolean setProperties(Properties props) {
        String str;
        long[] tmp;

        super.setProperties(props);
        str=props.getProperty("retransmit_timeout");
        if(str != null) {
            tmp=Util.parseCommaDelimitedLongs(str);
            props.remove("retransmit_timeout");
            if(tmp != null && tmp.length > 0) {
                retransmit_timeout=tmp;
            }
        }

        str=props.getProperty("gc_lag");
        if(str != null) {
            gc_lag=Integer.parseInt(str);
            if(gc_lag < 0) {
                log.error(ExternalStrings.NAKACK_NAKACKSETPROPERTIES_GC_LAG_CANNOT_BE_NEGATIVE_SETTING_IT_TO_0);
            }
            props.remove("gc_lag");
        }

        str=props.getProperty("max_xmit_size");
        if(str != null) {
            max_xmit_size=Long.parseLong(str);
            props.remove("max_xmit_size");
        }
        
        // GemStoneAddition - limit size of retransmission bursts
        str=props.getProperty("max_xmit_burst");
        if (str != null) {
          max_xmit_burst = Long.parseLong(str);
          props.remove("max_xmit_burst");
        }

        str=props.getProperty("use_mcast_xmit");
        if(str != null) {
            use_mcast_xmit=Boolean.valueOf(str).booleanValue();
            props.remove("use_mcast_xmit");
        }

        str=props.getProperty("discard_delivered_msgs");
        if(str != null) {
            discard_delivered_msgs=Boolean.valueOf(str).booleanValue();
            props.remove("discard_delivered_msgs");
        }

        str=props.getProperty("xmit_from_random_member");
        if(str != null) {
            xmit_from_random_member=Boolean.valueOf(str).booleanValue();
            props.remove("xmit_from_random_member");
        }

        str=props.getProperty("max_xmit_buf_size");
        if(str != null) {
            max_xmit_buf_size=Integer.parseInt(str);
            props.remove("max_xmit_buf_size");
        }

        str=props.getProperty("stats_list_size");
        if(str != null) {
            stats_list_size=Integer.parseInt(str);
            props.remove("stats_list_size");
        }
        
        //GemStoneAddition
        str = props.getProperty("max_sent_msgs_size");
        if (str != null) {
          max_sent_msgs_size = Long.parseLong(str);
          props.remove("max_sent_msgs_size");
        }

        if(xmit_from_random_member) {
            if(discard_delivered_msgs) {
                discard_delivered_msgs=false;
                log.warn("xmit_from_random_member set to true: changed discard_delivered_msgs to false");
            }
        }

        if(props.size() > 0) {
            log.error(ExternalStrings.NAKACK_NAKACKSETPROPERTIES_THESE_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);

            return false;
        }
        return true;
    }

    @Override // GemStoneAddition  
    public Map dumpStats() {
        Map retval=super.dumpStats();
        if(retval == null)
            retval=new HashMap();

        retval.put("xmit_reqs_received", Long.valueOf(xmit_reqs_received));
        retval.put("xmit_reqs_sent", Long.valueOf(xmit_reqs_sent));
        retval.put("xmit_rsps_received", Long.valueOf(xmit_rsps_received));
        retval.put("xmit_rsps_sent", Long.valueOf(xmit_rsps_sent));
        retval.put("missing_msgs_received", Long.valueOf(missing_msgs_received));

        retval.put("sent_msgs", printSentMsgs());

        StringBuffer sb=new StringBuffer();
        Map.Entry entry;
        Address addr;
        Object w;
        synchronized(received_msgs) {
            for(Iterator it=received_msgs.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                addr=(Address)entry.getKey();
                w=entry.getValue();
                sb.append(addr).append(": ").append(w.toString()).append('\n');
            }
        }

        retval.put("received_msgs", sb.toString());
        return retval;        
    }

    @Override // GemStoneAddition  
    public String printStats() {
        Map.Entry entry;
        Object key, val;
        StringBuffer sb=new StringBuffer();
        sb.append("sent:\n");
        for(Iterator it=sent.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            key=entry.getKey();
            if(key == null) key="<mcast dest>";
            val=entry.getValue();
            sb.append(key).append(": ").append(val).append("\n");
        }
        sb.append("\nreceived:\n");
        for(Iterator it=received.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            key=entry.getKey();
            val=entry.getValue();
            sb.append(key).append(": ").append(val).append("\n");
        }

        sb.append("\nXMIT_REQS sent:\n");
        XmitRequest tmp;
        for(Enumeration en=send_history.elements(); en.hasMoreElements();) {
            tmp=(XmitRequest)en.nextElement();
            sb.append(tmp).append("\n");
        }

        sb.append("\nMissing messages received\n");
        MissingMessage missing;
        for(Enumeration en=receive_history.elements(); en.hasMoreElements();) {
            missing=(MissingMessage)en.nextElement();
            sb.append(missing).append("\n");
        }

        return sb.toString();
    }



    @Override // GemStoneAddition  
    public Vector providedUpServices() {
        Vector retval=new Vector(5);
        retval.addElement(Integer.valueOf(Event.GET_DIGEST));
        retval.addElement(Integer.valueOf(Event.GET_DIGEST_STABLE));
        retval.addElement(Integer.valueOf(Event.GET_DIGEST_STATE));
        retval.addElement(Integer.valueOf(Event.SET_DIGEST));
        retval.addElement(Integer.valueOf(Event.MERGE_DIGEST));
        return retval;
    }


    @Override // GemStoneAddition  
    public Vector providedDownServices() {
        Vector retval=new Vector(2);
        retval.addElement(Integer.valueOf(Event.GET_DIGEST));
        retval.addElement(Integer.valueOf(Event.GET_DIGEST_STABLE));
        return retval;
    }


    @Override // GemStoneAddition  
    public void start() throws Exception {
        timer=stack != null ? stack.timer : null;
        if(timer == null) {
            throw new Exception("NAKACK.up(): timer is null");
        }
    }

    @Override // GemStoneAddition  
    public void stop() {
        removeAll();  // clears sent_msgs and destroys all NakReceiverWindows
    }


    /**
     * <b>Callback</b>. Called by superclass when event may be handled.<p> <b>Do not use <code>passDown()</code> in this
     * method as the event is passed down by default by the superclass after this method returns !</b>
     */
    @Override // GemStoneAddition  
    public void down(Event evt) {
//      long start; // GemStoneAddition
       Digest  digest;
        Vector  mbrs;

        switch(evt.getType()) {

        case Event.MSG:
            Message msg=(Message)evt.getArg();
            Address dest=msg.getDest();
            if(dest != null && !dest.isMulticastAddress()) {
                break; // unicast address: not null and not mcast, pass down unchanged
            }
            send(evt, msg);
            return;    // don't pass down the stack

        case Event.STABLE:  // generated by STABLE layer. Delete stable messages passed in arg
            stable((Digest)evt.getArg());
            return;  // do not pass down further (Bela Aug 7 2001)

        case Event.GET_DIGEST:
            digest=getDigest();
            passUp(new Event(Event.GET_DIGEST_OK, digest != null ? digest.copy() : null));
            return;

        case Event.GET_DIGEST_STABLE:
            digest=getDigestHighestDeliveredMsgs();
            passUp(new Event(Event.GET_DIGEST_STABLE_OK, digest != null ? digest.copy() : null));
            return;

        case Event.GET_DIGEST_STATE:
            digest=getDigest();
            passUp(new Event(Event.GET_DIGEST_STATE_OK, digest != null ? digest.copy() : null));
            return;

        case Event.SET_DIGEST:
            setDigest((Digest)evt.getArg());
            return;

        case Event.MERGE_DIGEST:
            mergeDigest((Digest)evt.getArg());
            return;

        case Event.CONFIG:
            passDown(evt);
            if(log.isDebugEnabled()) {
                log.debug("received CONFIG event: " + evt.getArg());
            }
            handleConfigEvent((HashMap)evt.getArg());
            return;

        case Event.TMP_VIEW:
            mbrs=((View)evt.getArg()).getMembers();
            members.clear();
            members.addAll(mbrs);
            adjustReceivers();
            break;

        case Event.VIEW_CHANGE:
            mbrs=((View)evt.getArg()).getMembers();
            members.clear();
            members.addAll(mbrs);
            this.viewId = ((View)evt.getArg()).getVid();
            adjustReceivers();
            is_server=true;  // check vids from now on

            Set tmp=new LinkedHashSet(members);
            tmp.add(null); // for null destination (= mcast)
            sent.keySet().retainAll(tmp);
            received.keySet().retainAll(tmp);
            break;

        case Event.BECOME_SERVER:
            is_server=true;
            break;

        case Event.DISCONNECT:
            leaving=true;
            removeAll();
            seqno=0;
            break;
        }

        passDown(evt);
    }



    /**
     * <b>Callback</b>. Called by superclass when event may be handled.<p> <b>Do not use <code>PassUp</code> in this
     * method as the event is passed up by default by the superclass after this method returns !</b>
     */
    @Override // GemStoneAddition  
    public void up(Event evt) {
//        NakAckHeader hdr;
//        Message msg;
        Digest digest;

//       long start; // GemStoneAddition
       switch(evt.getType()) {


        case Event.MSG:
            final Message msg=(Message)evt.getArg(); // GemStoneAddition - final
            final NakAckHeader hdr=(NakAckHeader)msg.getHeader(name);
            if(hdr == null)
                break;  // pass up (e.g. unicast msg)

            // discard messages while not yet server (i.e., until JOIN has returned)
            boolean isServer = is_server;
            if(!isServer || !isMember(msg.getSrc())) {
              GmsHeader ghdr = (GmsHeader)msg.getHeader("GMS");
              // GemstoneAddition:
              // see if this is a view that was sent out with NAKACK.  If so, we
              // really need to respond to it to keep from stalling the view handler
              if (ghdr != null && ghdr.type == GmsHeader.VIEW) {
                // bug 42152 - bypass stack and hand directly to GMS
                this.stack.findProtocol("GMS").up(evt);
              } else {
                if(trace)
                    log.trace("message was discarded (not yet server)");
              }
              if (!isServer) {
                return;
              }
            }

            // Changed by bela Jan 29 2003: we must not remove the header, otherwise
            // further xmit requests will fail !
            //hdr=(NakAckHeader)msg.removeHeader(getName());

            switch(hdr.type) {

            case NakAckHeader.MSG:
                handleMessage(msg, hdr);
                return;        // transmitter passes message up for us !

            case NakAckHeader.XMIT_REQ:
                if(hdr.range == null) {
                    if(log.isErrorEnabled()) {
                        log.error(ExternalStrings.NAKACK_XMIT_REQ_RANGE_OF_XMIT_MSG_IS_NULL_DISCARDING_REQUEST_FROM__0, msg.getSrc());
                    }
                    return;
                }
                handleXmitReq(msg.getSrc(), hdr.range.low, hdr.range.high, hdr.sender);
                return;

            case NakAckHeader.XMIT_RSP:
                if(trace)
                    log.trace("received missing messages " + hdr.range);
                handleXmitRsp(msg);
                 return;

            default:
                if(log.isErrorEnabled()) {
                    log.error(ExternalStrings.NAKACK_NAKACK_HEADER_TYPE__0__NOT_KNOWN_, hdr.type);
                }
                return;
            }

        case Event.STABLE:  // generated by STABLE layer. Delete stable messages passed in arg
            stable((Digest)evt.getArg());
            return;  // do not pass up further (Bela Aug 7 2001)

        case Event.GET_DIGEST:
            digest=getDigestHighestDeliveredMsgs();
            passDown(new Event(Event.GET_DIGEST_OK, digest));
            return;

        case Event.GET_DIGEST_STABLE:
            digest=getDigestHighestDeliveredMsgs();
            passDown(new Event(Event.GET_DIGEST_STABLE_OK, digest));
            return;

        case Event.SET_LOCAL_ADDRESS:
            local_addr=(Address)evt.getArg();
            break;

        case Event.CONFIG:
            passUp(evt);
            if(log.isDebugEnabled()) {
                log.debug("received CONFIG event: " + evt.getArg());
            }
            handleConfigEvent((HashMap)evt.getArg());
            return;
        }
        passUp(evt);
    }


  // GemStoneAddition - check to see if a sender is a member or not
  private boolean isMember(Address addr) {
    if (!this.members.contains(addr) && this.viewId != null) {
      // we may not have seen the view announcing this member
      return addr.getBirthViewId() > viewId.getId();
    }
    return true;
  }


    /* --------------------------------- Private Methods --------------------------------------- */

    private synchronized long getNextSeqno() {
        return seqno++;
    }
    
    /**
     * GemStoneAddition - return the current sequence number
     * @return the current sequence number
     */
    public long getCurrentSeqno() {
      return seqno-1;
    }
    
//    private void recalculate_send_msgs() {
//      long old_size = sent_msgs_size;
//      long new_size = 0;
//      
//      synchronized (sent_msgs) {
//        Iterator it = sent_msgs.keySet().iterator();
//        while (it.hasNext()) {
//          Long k = (Long)it.next();
//          Message m = (Message)sent_msgs.get(k);
//          new_size += m.size();
//        }
//      if (stack.gemfireStats != null)
//        stack.gemfireStats.incJgSentMessagesSize(new_size - old_size);
//      }
//    }

   /**
     * Adds the message to the sent_msgs table and then passes it down the stack. Change Bela Ban May 26 2002: we don't
     * store a copy of the message, but a reference ! This saves us a lot of memory. However, this also means that a
     * message should not be changed after storing it in the sent-table ! See protocols/DESIGN for details.
     */
    private void send(Event evt, Message msg) {
      synchronized(this) { // GemStone - fix for bug #34551
        long msg_id=getNextSeqno();
        if(trace)
            log.trace(local_addr.toString() + ": sending msg #" + msg_id);

        msg.putHeader(name, new NakAckHeader(NakAckHeader.MSG, msg_id));
        synchronized(sent_msgs) {
            if(Global.copy) {
                sent_msgs.put(Long.valueOf(msg_id), msg.copy());
            }
            else {
                sent_msgs.put(Long.valueOf(msg_id), msg);
            }
            // GemStoneAddition
            if (max_sent_msgs_size > 0) { // feature enabled
              if (sent_msgs.size() > max_sent_msgs_size) { // full
                try {
                  sent_msgs.wait(1);
                }
                catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                stack.gfPeerFunctions.incJgNAKACKwaits(1);
              } // full
            } // fetaure enabled
            stack.gfPeerFunctions.setJgSTABLEsentMessagesSize(sent_msgs.size());
        }
      } // synchronized(this)
      passDown(evt);
    }


    /**
     * GemStoneAddition - sync delivery to prevent race between mcast receiver thread
     * and ucast receiver thread delivering messages out of order
     */
    private final Object deliverySync = new Object();
    
    /**
     * Finds the corresponding NakReceiverWindow and adds the message to it (according to seqno). Then removes as many
     * messages as possible from the NRW and passes them up the stack. Discards messages from non-members.
     */
    private void handleMessage(Message msg, NakAckHeader hdr) {
//        long start; // GemStoneAddition
        NakReceiverWindow win;
        Message msg_to_deliver;
        Address sender=msg.getSrc();

        if(sender == null) {
            if(log.isErrorEnabled()) { // GemStoneAddition - verbose msg for debugging 36611 (bruce)
                log.error("sender of message is null: " + msg.toString(),
                    new Exception("Stack trace - please send to GemStone " +
                                "customer support and reference bug #36611"));
            }
            return;
        }

        if(trace) {
            StringBuffer sb=new StringBuffer('[');
            sb.append(local_addr).append("] received ").append(sender).append('#').append(hdr.seqno);
            log.trace(sb.toString());
        }

        // msg is potentially re-sent later as result of XMIT_REQ reception; that's why hdr is added !

        // Changed by bela Jan 29 2003: we currently don't resend from received msgs, just from sent_msgs !
        // msg.putHeader(getName(), hdr);

        synchronized(received_msgs) {
            win=(NakReceiverWindow)received_msgs.get(sender);
        }
        if(win == null) {  // discard message if there is no entry for sender
            if(leaving)
                return;
            if(warn) {
                StringBuffer sb=new StringBuffer('[');
                sb.append(local_addr).append("] discarded message from non-member ").append(sender);
                if(warn)
                    log.warn(sb.toString());
            }
            return;
        }
        synchronized(deliverySync) {
          win.add(hdr.seqno, msg);  // add in order, then remove and pass up as many msgs as possible
  
          while((msg_to_deliver=win.remove()) != null) {
              // Changed by bela Jan 29 2003: not needed (see above)
              //msg_to_deliver.removeHeader(getName());
            if (trace) {
              log.trace(local_addr.toString() + ": NAKACK dispatching " + msg_to_deliver);
            }
              passUp(new Event(Event.MSG, msg_to_deliver));
          }
        }
    }
    
    // GemStoneAddition - test of failure to transmit a large message.
    // It should throw a RetransmissionTooLargeException
    public boolean testRetransmitLargeMessage(byte[] bytesToSend) {
      IpAddress myAddress = (IpAddress)local_addr;
      int myPort = myAddress.getPort();
      Message msg = new Message(new IpAddress(myAddress.getIpAddress(), myPort-1), this.local_addr, bytesToSend, 0, bytesToSend.length);
      msg.putHeader(name, new NakAckHeader(NakAckHeader.XMIT_RSP, 0, 0));
      try {
        passDown(new Event(Event.MSG, msg));
      } catch (RetransmissionTooLargeException e) {
        return true;
      }
      return false;
    }


    /**
     * Retransmit from sent-table, called when XMIT_REQ is received. Bundles all messages to be xmitted into one large
     * message and sends them back with an XMIT_RSP header. Note that since we cannot count on a fragmentation layer
     * below us, we have to make sure the message doesn't exceed max_xmit_size bytes. If this is the case, we split the
     * message into multiple, smaller-chunked messages. But in most cases this still yields fewer messages than if each
     * requested message was retransmitted separately.
     *
     * @param xmit_requester        The sender of the XMIT_REQ, we have to send the requested copy of the message to this address
     * @param first_seqno The first sequence number to be retransmitted (<= last_seqno)
     * @param last_seqno  The last sequence number to be retransmitted (>= first_seqno)
     * @param original_sender The member who originally sent the messsage. Guaranteed to be non-null
     */
    void handleXmitReq(Address xmit_requester, long first_seqno, long last_seqno, Address original_sender) {
        Message m, tmp;
        LinkedList list;
        long size=0, marker=first_seqno, len;
        long burstSize=0; // GemStoneAddition - limit bursts of retransmissions
        NakReceiverWindow win=null;
        boolean      amISender; // am I the original sender ?

        // GemStoneAddition - ignore requests from those not in membership
        synchronized(this.members) {
          if (!isMember(xmit_requester)) {
            // request from someone who's not in membership - ignore it
            if (trace) {
              log.trace("Ignoring xmit request from " + xmit_requester + " who is not in membership");
            }
            return;
          }
        }

        if(trace) {
            StringBuffer sb=new StringBuffer();
            sb.append(local_addr).append(": received xmit request from ").append(xmit_requester).append(" for ");
            sb.append(original_sender).append(" [").append(first_seqno).append(" - ").append(last_seqno).append("]");
            log.trace(sb.toString());
        }

        if(first_seqno > last_seqno) {
            if(log.isErrorEnabled())
                log.error(ExternalStrings.NAKACK_FIRST_SEQNO__0___LAST_SEQNO__1__NOT_ABLE_TO_RETRANSMIT, new Object[] {Long.valueOf(first_seqno), Long.valueOf(last_seqno)});
            return;
        }

        if(stats) {
            xmit_reqs_received+=last_seqno - first_seqno +1;
            updateStats(received, xmit_requester, 1, 0, 0);
        }

        amISender=local_addr.equals(original_sender);
        if(!amISender)
            win=(NakReceiverWindow)received_msgs.get(original_sender);

        list=new LinkedList();
        for(long i=first_seqno; i <= last_seqno && (max_xmit_burst <= 0 || burstSize <= max_xmit_burst); i++) {  // GemStoneAddition - burst control
            if(amISender) {
                m=(Message)sent_msgs.get(Long.valueOf(i)); // no need to synchronize
            }
            else {
                m=win != null? win.get(i) : null;
            }
            if(m == null) {
                  // GemStone: don't log this.  It can happen when the other member leaves
//                if(log.isErrorEnabled() && !stack.getChannel().closing()) {
//                    StringBuffer sb=new StringBuffer();
//                    sb.append("(requester=").append(xmit_requester).append(", local_addr=").append(this.local_addr);
//                    sb.append(") message ").append(original_sender).append("::").append(i);
//                    sb.append(" not found in ").append((amISender? "sent" : "received")).append(" msgs. ");
//                    if(win != null) {
//                        sb.append("Received messages from ").append(original_sender).append(": ").append(win.toString());
//                    }
//                    else {
//                        sb.append("\nSent messages: ").append(printSentMsgs());
//                    }
//                    log.error(sb.toString());
//                }
               // GemStoneAddition - if the message isn't here anymore we need to send something to stop
               // further retransmission requests
                if (stack.gfPeerFunctions.isDisconnecting()) {
                  return;
            }
              // requested message is not present
              if (log.getLogWriter().fineEnabled()) {
                log.getLogWriter().fine("Retransmission request received from " + xmit_requester + " for a message that has already been discarded");
              }
                m = new Message();
                m.setDest(xmit_requester);
                m.setSrc(local_addr);
                NakAckHeader hdr = new NakAckHeader(NakAckHeader.MSG, i);
                m.putHeader(name, hdr);
//                continue;
            }
            len=m.size();
            size+=len;
            burstSize += len;
            if(size > max_xmit_size && list.size() > 0) { // changed from >= to > (yaron-r, bug #943709)
                // yaronr: added &&listSize()>0 since protocols between FRAG and NAKACK add headers, and message exceeds size.

                // size has reached max_xmit_size. go ahead and send message (excluding the current message)
                if(trace)
                    log.trace("xmitting msgs [" + marker + '-' + (i - 1) + "] to " + xmit_requester);
                sendXmitRsp(xmit_requester, (LinkedList)list.clone(), marker, i - 1);
                marker=i;
                list.clear();
                // fixed Dec 15 2003 (bela, patch from Joel Dice (dicej)), see explanantion under
                // bug report #854887
                size=len;
                burstSize=len;
            }
            if(Global.copy) {
                tmp=m.copy();
            }
            else {
                tmp=m;
            }
            // tmp.setDest(xmit_requester);
            // tmp.setSrc(local_addr);
            if(tmp.getSrc() == null)
                tmp.setSrc(local_addr);
//            if (trace) {
//              log.trace("xmitting message " + m); // GemStoneAddition - debugging size problem
//            }
            list.add(tmp);
        }

        if(list.size() > 0) {
            if(trace)
                log.trace("xmitting msgs [" + marker + '-' + last_seqno + "] to " + xmit_requester);
            sendXmitRsp(xmit_requester, (LinkedList)list.clone(), marker, last_seqno);
            list.clear();
        }
    }

    private static void updateStats(HashMap map, Address key, int req, int rsp, int missing) {
        Entry entry=(Entry)map.get(key);
        if(entry == null) {
            entry=new Entry();
            map.put(key, entry);
        }
        entry.xmit_reqs+=req;
        entry.xmit_rsps+=rsp;
        entry.missing_msgs_rcvd+=missing;
    }

    private void sendXmitRsp(Address dest, LinkedList xmit_list, long first_seqno, long last_seqno) {
        Buffer buf;
        if(xmit_list == null || xmit_list.size() == 0) {
            if(log.isErrorEnabled())
                log.error(ExternalStrings.NAKACK_XMIT_LIST_IS_EMPTY);
            return;
        }
        if(use_mcast_xmit)
            dest=null;

        if(stats) {
            xmit_rsps_sent+=xmit_list.size();
            updateStats(sent, dest, 0, 1, 0);
        }

        try {
            buf=Util.msgListToByteBuffer(xmit_list, dest);
            Message msg=new Message(dest, null, buf.getBuf(), buf.getOffset(), buf.getLength());
            msg.putHeader(name, new NakAckHeader(NakAckHeader.XMIT_RSP, first_seqno, last_seqno));
            passDown(new Event(Event.MSG, msg));
            stack.gfPeerFunctions.incMcastRetransmits(); // GemStoneAddition
        }
        // GemStoneAddition - if the buffer is too large, retry by sending each
        // message individually
        catch (RetransmissionTooLargeException e) {
          for (Message xmitMsg:  (List<Message>)xmit_list) {
            NakAckHeader xmitHdr = (NakAckHeader)xmitMsg.getHeader(name);
            long seqno = xmitHdr.seqno;
            Message msg;
            try {
              buf=Util.msgListToByteBuffer(Collections.singletonList(xmitMsg), null);
              msg=new Message(dest, null, buf.getBuf(), buf.getOffset(), buf.getLength());
              msg.putHeader(name, new NakAckHeader(NakAckHeader.XMIT_RSP, seqno, seqno));
              passDown(new Event(Event.MSG, msg));
              stack.gfPeerFunctions.incMcastRetransmits(); // GemStoneAddition
            } catch (IOException ex) {
              log.error(ExternalStrings.NAKACK_FAILED_MARSHALLING_XMIT_LIST, ex);
              continue;
            }
          }
        }
        catch(IOException ex) {
            log.error(ExternalStrings.NAKACK_FAILED_MARSHALLING_XMIT_LIST, ex);
        }
    }
    
    // GemStoneAddition - handle messages that are too large
    public static boolean isRetransmission(Message msg) { 
      NakAckHeader hdr = (NakAckHeader)msg.getHeader(name);
      if (hdr != null && hdr.type == NakAckHeader.XMIT_RSP) {
        return true;
      }
      return false;
    }

    // GemStoneAddition - handle messages that are too large
    public static class RetransmissionTooLargeException extends RuntimeException {
      public RetransmissionTooLargeException(String message) {
        super(message);
      }
    }


    private void handleXmitRsp(Message msg) {
        LinkedList list;
        Message m;

        if(msg == null) {
            if(warn)
                log.warn("message is null");
            return;
        }
        try {
            list=Util.byteBufferToMessageList(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
            if(list != null) {
                if(stats) {
                    xmit_rsps_received+=list.size();
                    updateStats(received, msg.getSrc(), 0, 1, 0);
                }
                for(Iterator it=list.iterator(); it.hasNext();) {
                    m=(Message)it.next();
                    up(new Event(Event.MSG, m));
                }
                list.clear();
            }
        }
        catch(Exception ex) {
            if(log.isErrorEnabled() && !ex.getClass().getSimpleName().equals("CancelException")) {
              // GemStoneAddition - debugging for 36612
              log.error("Exception caught while processing a message from " + msg.getSrc(), ex);
            }
        }
    }




    /**
     * Remove old members from NakReceiverWindows and add new members (starting seqno=0). Essentially removes all
     * entries from received_msgs that are not in <code>members</code>
     */
    private void adjustReceivers() {
        Address sender;
        NakReceiverWindow win;
        synchronized(received_msgs) {
          received_msgs_size = 0; // we will recalculate it

            // 1. Remove all senders in received_msgs that are not members anymore
            for(Iterator it=received_msgs.keySet().iterator(); it.hasNext();) {
                sender=(Address)it.next();
                if(!isMember(sender)) {
                    win=(NakReceiverWindow)received_msgs.get(sender);
                    win.reset();
                    if(log.isDebugEnabled()) {
                        log.debug("removing " + sender + " from received_msgs (not member anymore)");
                    }
                    it.remove();
                }
                else {
                  // statistics gather
                  win = (NakReceiverWindow)received_msgs.get(sender);
                  received_msgs_size += win.unsafeGetSize();
                  stack.gfPeerFunctions.setJgSTABLEreceivedMessagesSize(received_msgs_size);
                }
            }

            // 2. Add newly joined members to received_msgs (starting seqno=0)
            for(int i=0; i < members.size(); i++) {
                sender=(Address)members.elementAt(i);
                if(!received_msgs.containsKey(sender)) {
                    win=createNakReceiverWindow(sender, 0);
                    received_msgs.put(sender, win);
                    // It's zero length, no need to update received_msgs_size
                }
            }
            
            // 3. update statistic
            stack.gfPeerFunctions.setJgSTABLEreceivedMessagesSize(received_msgs_size);
        }
    }


    /**
     * Returns a message digest: for each member P the highest seqno received from P is added to the digest.
     */
    private Digest getDigest() {
        Digest digest;
        Address sender;
        Range range;

        digest=new Digest(members.size());
        for(int i=0; i < members.size(); i++) {
            sender=(Address)members.elementAt(i);
            range=getLowestAndHighestSeqno(sender, false);  // get the highest received seqno
            if(range == null) {
                if(log.isErrorEnabled() && !stack.getChannel().closing()) { // GemStoneAddition - don't log this if the channel is closing
                    log.error(ExternalStrings.NAKACK_RANGE_IS_NULL);
                }
                continue;
            }
            digest.add(sender, range.low, range.high);  // add another entry to the digest
        }
        return digest;
    }


    /**
     * Returns a message digest: for each member P the highest seqno received from P <em>without a gap</em> is added to
     * the digest. E.g. if the seqnos received from P are [+3 +4 +5 -6 +7 +8], then 5 will be returned. Also, the
     * highest seqno <em>seen</em> is added. The max of all highest seqnos seen will be used (in STABLE) to determine
     * whether the last seqno from a sender was received (see "Last Message Dropped" topic in DESIGN).
     */
    private Digest getDigestHighestDeliveredMsgs() {
        Digest digest;
        Address sender;
        Range range;
        long high_seqno_seen;

        digest=new Digest(members.size());
        for(int i=0; i < members.size(); i++) {
            sender=(Address)members.elementAt(i);
            range=getLowestAndHighestSeqno(sender, true);  // get the highest deliverable seqno
            if(range == null) {
                if(log.isErrorEnabled() && !stack.getChannel().closing()) { // GemStoneAddition - don't log this if the channel is closing
                    log.error(ExternalStrings.NAKACK_RANGE_IS_NULL_2); // GemStoneAddition to distinguish from other place this error is generated
                }
                continue;
            }
            high_seqno_seen=getHighSeqnoSeen(sender);
            digest.add(sender, range.low, range.high, high_seqno_seen);  // add another entry to the digest
        }
        return digest;
    }


    /**
     * Creates a NakReceiverWindow for each sender in the digest according to the sender's seqno. If NRW already exists,
     * reset it.
     */
    private void setDigest(Digest d) {
        if(d == null || d.senders == null) {
            if(log.isErrorEnabled()) {
                log.error(ExternalStrings.NAKACK_DIGEST_OR_DIGESTSENDERS_IS_NULL);
            }
            return;
        }
        if (trace) log.trace("installing NAKACK digest " + d);

        clear();

        Map.Entry entry;
        Address sender;
        com.gemstone.org.jgroups.protocols.pbcast.Digest.Entry val;
        long initial_seqno;
        NakReceiverWindow win;

        for(Iterator it=d.senders.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            sender=(Address)entry.getKey();
            val=(com.gemstone.org.jgroups.protocols.pbcast.Digest.Entry)entry.getValue();

            if(sender == null || val == null) {
                if(warn) {
                    log.warn("sender or value is null");
                }
                continue;
            }
            initial_seqno=val.high_seqno;
            win=createNakReceiverWindow(sender, initial_seqno);
            synchronized(received_msgs) {
                received_msgs.put(sender, win);
                // all new entries are of size 0, no need to increment received_msgs_size
            }
        }
    }


    /**
     * For all members of the digest, adjust the NakReceiverWindows in the received_msgs hashtable. If the member
     * already exists, sets its seqno to be the max of the seqno and the seqno of the member in the digest. If no entry
     * exists, create one with the initial seqno set to the seqno of the member in the digest.
     */
    private void mergeDigest(Digest d) {
        if(d == null || d.senders == null) {
            if(log.isErrorEnabled()) {
                log.error(ExternalStrings.NAKACK_DIGEST_OR_DIGESTSENDERS_IS_NULL);
            }
            return;
        }

        Map.Entry entry;
        Address sender;
        com.gemstone.org.jgroups.protocols.pbcast.Digest.Entry val;
        NakReceiverWindow win;
        long initial_seqno;

        for(Iterator it=d.senders.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            sender=(Address)entry.getKey();
            val=(com.gemstone.org.jgroups.protocols.pbcast.Digest.Entry)entry.getValue();

            if(sender == null || val == null) {
                if(warn) {
                    log.warn("sender or value is null");
                }
                continue;
            }
            initial_seqno=val.high_seqno;
            synchronized(received_msgs) {
                win=(NakReceiverWindow)received_msgs.get(sender);
                if(win == null) {
                    win=createNakReceiverWindow(sender, initial_seqno);
                    // it's empty, no need to increment received_msgs_size
                }
                else {
                    if(win.getHighestReceived() < initial_seqno) {
                        received_msgs_size -= win.unsafeGetSize();
                        stack.gfPeerFunctions.setJgSTABLEreceivedMessagesSize(received_msgs_size);
                        win.reset();
                        received_msgs.remove(sender);
                        win=createNakReceiverWindow(sender, initial_seqno);
                        received_msgs.put(sender, win);
                    }
                }
                stack.gfPeerFunctions.setJgSTABLEreceivedMessagesSize(received_msgs_size);
                
            }
        }
    }


    private NakReceiverWindow createNakReceiverWindow(Address sender, long initial_seqno) {
        NakReceiverWindow win=new NakReceiverWindow(sender, this, initial_seqno, timer);
        win.setRetransmitTimeouts(retransmit_timeout);
        win.setDiscardDeliveredMessages(discard_delivered_msgs);
        win.setMaxXmitBufSize(this.max_xmit_buf_size);
        if(stats)
            win.setListener(this);
        return win;
    }


    /**
     * Returns the lowest seqno still in cache (so it can be retransmitted) and the highest seqno received so far.
     *
     * @param sender       The address for which the highest and lowest seqnos are to be retrieved
     * @param stop_at_gaps If true, the highest seqno *deliverable* will be returned. If false, the highest seqno
     *                     *received* will be returned. E.g. for [+3 +4 +5 -6 +7 +8], the highest_seqno_received is 8,
     *                     whereas the higheset_seqno_seen (deliverable) is 5.
     */
    private Range getLowestAndHighestSeqno(Address sender, boolean stop_at_gaps) {
        Range r=null;
        NakReceiverWindow win;

        if(sender == null) {
            if(log.isErrorEnabled()) {
                log.error(ExternalStrings.NAKACK_SENDER_IS_NULL);
            }
            return r;
        }
        synchronized(received_msgs) {
            win=(NakReceiverWindow)received_msgs.get(sender);
        }
        if(win == null) {
            if(log.isErrorEnabled() && !stack.getChannel().closing()) {  // GemStoneAddition - don't log if we're shutting down
                log.error(ExternalStrings.NAKACK_SENDER__0__NOT_FOUND_IN_RECEIVED_MSGS, sender);
            }
            return r;
        }
        if(stop_at_gaps) {
            r=new Range(win.getLowestSeen(), win.getHighestSeen());       // deliverable messages (no gaps)
        }
        else {
            r=new Range(win.getLowestSeen(), win.getHighestReceived() + 1); // received messages
        }
        return r;
    }


    /**
     * Returns the highest seqno seen from sender. E.g. if we received 1, 2, 4, 5 from P, then 5 will be returned
     * (doesn't take gaps into account). If we are the sender, we will return the highest seqno <em>sent</em> rather
     * then <em>received</em>
     */
    private long getHighSeqnoSeen(Address sender) {
        NakReceiverWindow win;
        long ret=0;

        if(sender == null) {
            if(log.isErrorEnabled()) {
                log.error(ExternalStrings.NAKACK_SENDER_IS_NULL);
            }
            return ret;
        }
        if(sender.equals(local_addr)) {
            return seqno - 1;
        }

        synchronized(received_msgs) {
            win=(NakReceiverWindow)received_msgs.get(sender);
        }
        if(win == null) {
          // GemStoneAddition - this can happen in the normal course of events if we haven't
          // received a digest that has the sender and haven't received any messages from
          // the sender that made it to NAKACK
//            if(log.isErrorEnabled()&& !stack.getChannel().closing()) {  // GemStoneAddition - don't log if we're shutting down
//                log.error("sender " + sender + " not found in received_msgs (2)"); // GemStoneAddition - to distinguish from other message
//            }
            return ret;
        }
        ret=win.getHighestReceived();
        return ret;
    }


    /**
     * GemStoneAddition<br>
     * Returns the highest seqno dispatched for the given sender. E.g. if we received 1, 2, 4, 5 from P, then 2 will be returned
     * (takes gaps into account). If we are the sender, we will return the highest seqno <em>sent</em> rather
     * then <em>received</em>
     */
    public long getHighSeqnoDispatched(Address sender) {
        NakReceiverWindow win;
        long ret=0;

        if(sender == null) {
            if(log.isErrorEnabled()) {
                log.error(ExternalStrings.NAKACK_SENDER_IS_NULL);
            }
            return ret;
        }
        if(sender.equals(local_addr)) {
            return seqno - 1;
        }

        synchronized(received_msgs) {
            win=(NakReceiverWindow)received_msgs.get(sender);
        }
        if(win == null) {
            if(log.isErrorEnabled()&& !stack.getChannel().closing()) {  // GemStoneAddition - don't log if we're shutting down
                log.error(ExternalStrings.NAKACK_SENDER__0__NOT_FOUND_IN_RECEIVED_MSGS_2, sender); // GemStoneAddition - to distinguish from other message
            }
            return ret;
        }
        ret=win.getHighestSeen();
        return ret;
    }


    /**
     * Garbage collect messages that have been seen by all members. Update sent_msgs: for the sender P in the digest
     * which is equal to the local address, garbage collect all messages <= seqno at digest[P]. Update received_msgs:
     * for each sender P in the digest and its highest seqno seen SEQ, garbage collect all delivered_msgs in the
     * NakReceiverWindow corresponding to P which are <= seqno at digest[P].
     */
    private void stable(Digest d) {
        NakReceiverWindow recv_win;
        long my_highest_rcvd;        // highest seqno received in my digest for a sender P
        long stability_highest_rcvd; // highest seqno received in the stability vector for a sender P

        if(members == null || local_addr == null || d == null) {
            if(warn)
                log.warn("members, local_addr or digest are null !");
            return;
        }

        if(trace) {
            log.trace("received stable digest " + d);
        }

        Map.Entry entry;
        Address sender;
        com.gemstone.org.jgroups.protocols.pbcast.Digest.Entry val;
        long high_seqno_delivered, high_seqno_received;

        for(Iterator it=d.senders.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            sender=(Address)entry.getKey();
            if(sender == null)
                continue;
            val=(com.gemstone.org.jgroups.protocols.pbcast.Digest.Entry)entry.getValue();
            high_seqno_delivered=val.high_seqno;
            high_seqno_received=val.high_seqno_seen;


            // check whether the last seqno received for a sender P in the stability vector is > last seqno
            // received for P in my digest. if yes, request retransmission (see "Last Message Dropped" topic
            // in DESIGN)
            synchronized(received_msgs) {
                recv_win=(NakReceiverWindow)received_msgs.get(sender);
            }
            if(recv_win != null) {
                my_highest_rcvd=recv_win.getHighestReceived();
                stability_highest_rcvd=high_seqno_received;

                if(stability_highest_rcvd >= 0 && stability_highest_rcvd > my_highest_rcvd) {
                    if(trace) {
                        log.trace("my_highest_rcvd (" + my_highest_rcvd + ") < stability_highest_rcvd (" +
                                stability_highest_rcvd + "): requesting retransmission of " +
                                sender + '#' + stability_highest_rcvd);
                    }
                    retransmit(stability_highest_rcvd, stability_highest_rcvd, sender);
                }
            }

            high_seqno_delivered-=gc_lag;
            if(high_seqno_delivered < 0) {
                continue;
            }

            if(trace)
                log.trace("deleting msgs <= " + high_seqno_delivered + " from " + sender);

            // garbage collect from sent_msgs if sender was myself
            if(sender.equals(local_addr)) {
                synchronized(sent_msgs) {
                    // gets us a subset from [lowest seqno - seqno]
                    SortedMap stable_keys=sent_msgs.headMap(Long.valueOf(high_seqno_delivered));
                    if(stable_keys != null) {
//                      if (stack.gemfireStats != null) {
//                        Iterator i = stable_keys.keySet().iterator();
//                        long sz = 0;
//                        while (i.hasNext()) {
//                          Long k = (Long)i.next();
//                          Message m = (Message)sent_msgs.get(k);
//                          sz += m.size();
//                        }
//                        stack.gemfireStats.incJgSentMessagesSize(-sz);
//                      }
                      stable_keys.clear(); // this will modify sent_msgs directly
                      sent_msgs.notifyAll(); // GemStoneAddition
                      stack.gfPeerFunctions.setJgSTABLEsentMessagesSize(sent_msgs.size());
                    }
                }
            }

            // delete *delivered* msgs that are stable
            // recv_win=(NakReceiverWindow)received_msgs.get(sender);
            if(recv_win != null) {
                recv_win.stable(high_seqno_delivered);  // delete all messages with seqnos <= seqno
            }
        }
    }



    /* ---------------------- Interface Retransmitter.RetransmitCommand ---------------------- */


    public Address getDest() {
      return null;
    }
    
    /**
     * Implementation of Retransmitter.RetransmitCommand. Called by retransmission thread when gap is detected.
     */
    public void retransmit(long first_seqno, long last_seqno, Address sender) {
        NakAckHeader hdr;
        Message retransmit_msg;
        Address dest=sender; // to whom do we send the XMIT request ?

        if(xmit_from_random_member && !local_addr.equals(sender)) {
            Address random_member=(Address)Util.pickRandomElement(members);
            if(random_member != null && !local_addr.equals(random_member)) {
                dest=random_member;
                if(trace)
                    log.trace("picked random member " + dest + " to send XMIT request to");
            }
        }

        hdr=new NakAckHeader(NakAckHeader.XMIT_REQ, first_seqno, last_seqno, sender);
        retransmit_msg=new Message(dest, null, null);
        if(trace)
            log.trace(local_addr + ": sending XMIT_REQ ([" + first_seqno + ", " + last_seqno + "]) to " + dest);
        retransmit_msg.putHeader(name, hdr);
        passDown(new Event(Event.MSG, retransmit_msg));
        stack.gfPeerFunctions.incMcastRetransmitRequests();
        if(stats) {
            xmit_reqs_sent+=last_seqno - first_seqno +1;
            updateStats(sent, dest, 1, 0, 0);
            for(long i=first_seqno; i <= last_seqno; i++) {
                XmitRequest req=new XmitRequest(sender, i, dest);
                send_history.add(req);
            }
        }
    }
    /* ------------------- End of Interface Retransmitter.RetransmitCommand -------------------- */



    /* ----------------------- Interface NakReceiverWindow.Listener ---------------------- */
    public void missingMessageReceived(long mseqno, Message msg) {
        if(stats) {
            missing_msgs_received++;
            updateStats(received, msg.getSrc(), 0, 0, 1);
            MissingMessage missing=new MissingMessage(msg.getSrc(), mseqno);
            receive_history.add(missing);
        }
    }
    /* ------------------- End of Interface NakReceiverWindow.Listener ------------------- */

    private void clear() {
        NakReceiverWindow win;

        // changed April 21 2004 (bela): SourceForge bug# 938584. We cannot delete our own messages sent between
        // a join() and a getState(). Otherwise retransmission requests from members who missed those msgs might
        // fail. Not to worry though: those msgs will be cleared by STABLE (message garbage collection)

        // sent_msgs.clear();

        synchronized(received_msgs) {
            for(Iterator it=received_msgs.values().iterator(); it.hasNext();) {
                win=(NakReceiverWindow)it.next();
                win.reset();
            }
             received_msgs.clear();
             received_msgs_size = 0;
             stack.gfPeerFunctions.setJgSTABLEreceivedMessagesSize(received_msgs_size);
        }
    }


    private void removeAll() {
        NakReceiverWindow win;

        synchronized(sent_msgs) {
          sent_msgs.clear();
            stack.gfPeerFunctions.setJgSTABLEsentMessagesSize(sent_msgs.size());
        }

        synchronized(received_msgs) {
            for(Iterator it=received_msgs.values().iterator(); it.hasNext();) {
                win=(NakReceiverWindow)it.next();
                win.destroy();
            }
            received_msgs.clear();
            received_msgs_size = 0;
            stack.gfPeerFunctions.setJgSTABLEreceivedMessagesSize(received_msgs_size);
        }
    }


   public String printMessages() {
        StringBuffer ret=new StringBuffer();
        Map.Entry entry;
        Address addr;
        Object w;

       ret.append("\nsent_msgs: ").append(printSentMsgs());
        ret.append("\nreceived_msgs:\n");
        synchronized(received_msgs) {
            for(Iterator it=received_msgs.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                addr=(Address)entry.getKey();
                w=entry.getValue();
                ret.append(addr).append(": ").append(w.toString()).append('\n');
            }
        }
        return ret.toString();
    }


    public String printSentMsgs() {
        StringBuffer sb=new StringBuffer();
        Long min_seqno, max_seqno;
        synchronized(sent_msgs) {
            min_seqno=sent_msgs.size() > 0 ? (Long)sent_msgs.firstKey() : Long.valueOf(0);
            max_seqno=sent_msgs.size() > 0 ? (Long)sent_msgs.lastKey() : Long.valueOf(0);
        }
        sb.append('[').append(min_seqno).append(" - ").append(max_seqno).append("] (" + sent_msgs.size() + ")");
        return sb.toString();
    }


    private void handleConfigEvent(HashMap map) {
        if(map == null) {
            return;
        }
        if(map.containsKey("frag_size")) {
            max_xmit_size=((Integer)map.get("frag_size")).intValue();
            if(log.isInfoEnabled()) {
                log.info(ExternalStrings.NAKACK_MAX_XMIT_SIZE_0, max_xmit_size);
            }
        }
    }


    static class Entry  {
        long xmit_reqs, xmit_rsps, missing_msgs_rcvd;

        @Override // GemStoneAddition  
        public String toString() {
            StringBuffer sb=new StringBuffer();
            sb.append(xmit_reqs).append(" xmit_reqs").append(", ").append(xmit_rsps).append(" xmit_rsps");
            sb.append(", ").append(missing_msgs_rcvd).append(" missing msgs");
            return sb.toString();
        }
    }

    static class XmitRequest  {
        Address original_sender; // original sender of message
        long    seq, timestamp=System.currentTimeMillis();
        Address xmit_dest;       // destination to which XMIT_REQ is sent, usually the original sender

        XmitRequest(Address original_sender, long seqno, Address xmit_dest) {
            this.original_sender=original_sender;
            this.xmit_dest=xmit_dest;
            this.seq=seqno;
        }

        @Override // GemStoneAddition  
        public String toString() {
            StringBuffer sb=new StringBuffer();
            sb.append(new Date(timestamp)).append(": ").append(original_sender).append(" #").append(seq);
            sb.append(" (XMIT_REQ sent to ").append(xmit_dest).append(")");
            return sb.toString();
        }
    }

    static class MissingMessage  {
        Address original_sender;
        long    seq, timestamp=System.currentTimeMillis();

        MissingMessage(Address original_sender, long seqno) {
            this.original_sender=original_sender;
            this.seq=seqno;
        }

        @Override // GemStoneAddition  
        public String toString() {
            StringBuffer sb=new StringBuffer();
            sb.append(new Date(timestamp)).append(": ").append(original_sender).append(" #").append(seq);
            return sb.toString();
        }
    }

    /* ----------------------------- End of Private Methods ------------------------------------ */


}
