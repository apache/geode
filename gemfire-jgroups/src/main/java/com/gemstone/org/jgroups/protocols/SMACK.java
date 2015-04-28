/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: SMACK.java,v 1.12 2005/08/11 12:43:47 belaban Exp $

package com.gemstone.org.jgroups.protocols;



import com.gemstone.org.jgroups.*;
import com.gemstone.org.jgroups.stack.AckMcastSenderWindow;
import com.gemstone.org.jgroups.stack.AckReceiverWindow;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;




// @todo Initial mcast to announce new member (for view change)
/**
 * Simple Multicast ACK protocol. A positive acknowledgment-based protocol for reliable delivery of
 * multicast messages, which does not need any group membership service.
 * Basically works as follows:
 * <ul>
 * <li>Sender S sends multicast message M</li>
 * <li>When member P receives M, it sends back a unicast ack to S</li>
 * <li>When S receives the ack from P, it checks whether P is in its
 *     membership list. If not, P will be added. This is necessary to retransmit the next message
 *     sent to P.</li>
 * <li>When S sends a multicast message M, all members are added to a
 *     retransmission entry (containing all members to which the message
 *     was sent), which is added to a hashmap (keyed by seqno). Whenever
 *     an ack is received from receiver X, X will be removed from the
 *     retransmission list for the given seqno. When the retransmission
 *     list is empty, the seqno will be removed from the hashmap.</li>
 * <li>A retransmitter thread in the sender periodically retransmits
 *     (either via unicast, or multicast) messages for which no ack has
 *     been received yet</li>
 * <li>When a max number of (unsuccessful) retransmissions have been
 *     exceeded, all remaining members for that seqno are removed from
 *     the local membership, and the seqno is removed from te hashmap,
 *     ceasing all retransmissions</li>
 * </ul>
 * Advantage of this protocol: no group membership necessary, fast.
 * @author Bela Ban Aug 2002
 * @version $Revision: 1.12 $
 * <BR> Fix membershop bug: start a, b, kill b, restart b: b will be suspected by a.
 */
public class SMACK extends Protocol implements AckMcastSenderWindow.RetransmitCommand {
    long[]                 timeout={1000,2000,3000};  // retransmit timeouts (for AckMcastSenderWindow)
//    int                    max_xmits=10;              // max retransmissions (if still no ack, member will be removed) GemStoneAddition (omitted)
    final Vector                 members=new Vector();      // contains Addresses
    AckMcastSenderWindow   sender_win=null;
    final HashMap                receivers=new HashMap();   // keys=sender (Address), values=AckReceiverWindow
    final HashMap                xmit_table=new HashMap();  // keeps track of num xmits / member (keys: mbr, val:num)
    Address                local_addr=null;           // my own address
    long                   seqno=1;                   // seqno for msgs sent by this sender
    long                   vid=1;                     // for the fake view changes
    boolean                print_local_addr=true;
    static final String    name="SMACK";
    
    



    public SMACK() {
    }

    @Override // GemStoneAddition  
    public String getName() {
        return name;
    }


    @Override // GemStoneAddition  
    public boolean setProperties(Properties props) {
        String str;
        long[] tmp;

        super.setProperties(props);
        str=props.getProperty("print_local_addr");
        if(str != null) {
            print_local_addr=Boolean.valueOf(str).booleanValue();
            props.remove("print_local_addr");
        }

        str=props.getProperty("timeout");
        if(str != null) {
            tmp=Util.parseCommaDelimitedLongs(str);
            props.remove("timeout");
            if(tmp != null && tmp.length > 0)
                timeout=tmp;
        }

        str=props.getProperty("max_xmits");
        if(str != null) {
//            max_xmits=Integer.parseInt(str); GemStoneAddition
            props.remove("max_xmits");
        }


        if(props.size() > 0) {
            log.error(ExternalStrings.SMACK_SMACKSETPROPERTIES_THE_FOLLOWING_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);

            return false;
        }
        return true;
    }


    @Override // GemStoneAddition  
    public void stop() {
        AckReceiverWindow win;
        if(sender_win != null) {
            sender_win.stop();
            sender_win=null;
        }
        for(Iterator it=receivers.values().iterator(); it.hasNext();) {
            win=(AckReceiverWindow)it.next();
            win.reset();
        }
        receivers.clear();
    }


    @Override // GemStoneAddition  
    public void up(Event evt) {
        Address sender;

        switch(evt.getType()) {

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                addMember(local_addr);
                if(print_local_addr) {
                    System.out.println("\n-------------------------------------------------------\n" +
                                       "GMS: address is " + local_addr +
                                       "\n-------------------------------------------------------");
                }
                break;

            case Event.CONNECT_OK:
                passUp(evt);
                sender_win=new AckMcastSenderWindow(this, timeout);

                // send join announcement
                Message join_msg=new Message();
                join_msg.putHeader(name, new SmackHeader(SmackHeader.JOIN_ANNOUNCEMENT, -1));
                passDown(new Event(Event.MSG, join_msg));
                return;

            case Event.SUSPECT:

                    if(log.isInfoEnabled()) log.info(ExternalStrings.SMACK_REMOVING_SUSPECTED_MEMBER__0, evt.getArg());
                removeMember((Address)evt.getArg());
                break;

            case Event.MSG:
                Message msg=(Message)evt.getArg(), tmp_msg;
                if(msg == null) break;
                sender=msg.getSrc();
                SmackHeader hdr=(SmackHeader)msg.removeHeader(name);
                if(hdr == null) // is probably a unicast message
                    break;
                switch(hdr.type) {
                    case SmackHeader.MCAST: // send an ack, then pass up (if not already received)
                        Long tmp_seqno;
                        AckReceiverWindow win;
                        Message ack_msg=new Message(sender, null, null);

                        ack_msg.putHeader(name, new SmackHeader(SmackHeader.ACK, hdr.seqno));
                        passDown(new Event(Event.MSG, ack_msg));

                        tmp_seqno=Long.valueOf(hdr.seqno);

                        if(trace)
                            log.trace("received #" + tmp_seqno + " from " + sender);

                        win=(AckReceiverWindow)receivers.get(sender);
                        if(win == null) {
                            addMember(sender);
                            win=new AckReceiverWindow(hdr.seqno);
                            receivers.put(sender, win);
                        }
                        win.add(hdr.seqno, msg);

                        // now remove as many messages as possible
                        while((tmp_msg=win.remove()) != null)
                            passUp(new Event(Event.MSG, tmp_msg));
                        return;

                    case SmackHeader.ACK:
                        addMember(msg.getSrc());
                        sender_win.ack(hdr.seqno, msg.getSrc());
                        sender_win.clearStableMessages();
                        if(trace)
                            log.trace("received ack for #" + hdr.seqno + " from " + msg.getSrc());
                        return;

                    case SmackHeader.JOIN_ANNOUNCEMENT:

                            if(log.isInfoEnabled()) log.info(ExternalStrings.SMACK_RECEIVED_JOIN_ANNOUNCEMENT_BY__0, msg.getSrc());

                        if(!containsMember(sender)) {
                            Message join_rsp=new Message(sender, null, null);
                            join_rsp.putHeader(name, new SmackHeader(SmackHeader.JOIN_ANNOUNCEMENT, -1));
                            passDown(new Event(Event.MSG, join_rsp));
                        }
                        addMember(sender);
                        return;

                    case SmackHeader.LEAVE_ANNOUNCEMENT:

                            if(log.isInfoEnabled()) log.info(ExternalStrings.SMACK_RECEIVED_LEAVE_ANNOUNCEMENT_BY__0, msg.getSrc());

                        removeMember(sender);
                        return;

                    default:
                        if(warn) log.warn("detected SmackHeader with invalid type: " + hdr);
                        break;
                }
                break;
        }

        passUp(evt);
    }


    @Override // GemStoneAddition  
    public void down(Event evt) {
        Message leave_msg;

        switch(evt.getType()) {

            case Event.DISCONNECT:
                leave_msg=new Message();
                leave_msg.putHeader(name, new SmackHeader(SmackHeader.LEAVE_ANNOUNCEMENT, -1));
                passDown(new Event(Event.MSG, leave_msg));
                // passUp(new Event(Event.DISCONNECT_OK));
                break;

            case Event.CONNECT:
                //passUp(new Event(Event.CONNECT_OK));

                // Do not send JOIN_ANOUNCEMENT here, don't know yet if the transport is OK.
                // Send it later when handling CONNECT_OK from below

//                 sender_win=new AckMcastSenderWindow(this, timeout);
//                 // send join announcement
//                 Message join_msg=new Message();
//                 join_msg.putHeader(name, new SmackHeader(SmackHeader.JOIN_ANNOUNCEMENT, -1));
//                 passDown(new Event(Event.MSG, join_msg));
//                 return;

                break;


// add a header with the current sequence number and increment seqno
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                if(msg == null) break;
                if(msg.getDest() == null || msg.getDest().isMulticastAddress()) {
                    msg.putHeader(name, new SmackHeader(SmackHeader.MCAST, seqno));
                    sender_win.add(seqno, msg, (Vector)members.clone());
                    if(trace) log.trace("sending mcast #" + seqno);
                    seqno++;
                }
                break;
        }

        passDown(evt);
    }



    /* ----------------------- Interface AckMcastSenderWindow.RetransmitCommand -------------------- */

    public void retransmit(long seqno, Message msg, Address dest) {
        msg.setDest(dest);
        if(log.isInfoEnabled()) log.info(ExternalStrings.SMACK_0__MSG_1, 
          new Object[] {Long.valueOf(seqno), msg});
        passDown(new Event(Event.MSG, msg));
    }

    /* -------------------- End of Interface AckMcastSenderWindow.RetransmitCommand ---------------- */




    public static class SmackHeader extends Header  {
        public static final int MCAST=1;
        public static final int ACK=2;
        public static final int JOIN_ANNOUNCEMENT=3;
        public static final int LEAVE_ANNOUNCEMENT=4;

        int type=0;
        long seqno=-1;

        public SmackHeader() {
        }

        public SmackHeader(int type, long seqno) {
            this.type=type;
            this.seqno=seqno;
        }


        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(type);
            out.writeLong(seqno);
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readInt();
            seqno=in.readLong();
        }


        @Override // GemStoneAddition  
        public String toString() {
            switch(type) {
                case MCAST:
                    return "MCAST";
                case ACK:
                    return "ACK";
                case JOIN_ANNOUNCEMENT:
                    return "JOIN_ANNOUNCEMENT";
                case LEAVE_ANNOUNCEMENT:
                    return "LEAVE_ANNOUNCEMENT";
                default:
                    return "<unknown>";
            }
        }
    }


    /* ------------------------------------- Private methods --------------------------------------- */
    void addMember(Address mbr) {
        synchronized(members) {
            if(mbr != null && !members.contains(mbr)) {
                Object tmp;
                View new_view;
                members.addElement(mbr);
                tmp=members.clone();
                if(trace)
                    log.trace("added " + mbr + ", members=" + tmp);
                new_view=new View(new ViewId(local_addr, vid++), (Vector)tmp);
                passUp(new Event(Event.VIEW_CHANGE, new_view));
                passDown(new Event(Event.VIEW_CHANGE, new_view));
            }
        }
    }

    void removeMember(Address mbr) {
        synchronized(members) {
            if(mbr != null) {
                Object tmp;
                View new_view;
                members.removeElement(mbr);
                tmp=members.clone();
                if(trace)
                    log.trace("removed " + mbr + ", members=" + tmp);
                new_view=new View(new ViewId(local_addr, vid++), (Vector)tmp);
                passUp(new Event(Event.VIEW_CHANGE, new_view));
                passDown(new Event(Event.VIEW_CHANGE, new_view));
                if(sender_win != null)
                    sender_win.remove(mbr); // causes retransmissions to mbr to stop
            }
        }
    }


    boolean containsMember(Address mbr) {
        synchronized(members) {
            return mbr != null && members.contains(mbr);
        }
    }

    /* --------------------------------- End of Private methods ------------------------------------ */

}
