/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: FD_PROB.java,v 1.9 2005/08/11 12:43:47 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.*;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;


/**
 * Probabilistic failure detection protocol based on "A Gossip-Style Failure Detection Service"
 * by Renesse, Minsky and Hayden.<p>
 * Each member maintains a list of all other members: for each member P, 2 data are maintained, a heartbeat
 * counter and the time of the last increment of the counter. Each member periodically sends its own heartbeat
 * counter list to a randomly chosen member Q. Q updates its own heartbeat counter list and the associated
 * time (if counter was incremented). Each member periodically increments its own counter. If, when sending
 * its heartbeat counter list, a member P detects that another member Q's heartbeat counter was not incremented
 * for timeout seconds, Q will be suspected.<p>
 * This protocol can be used both with a PBCAST *and* regular stacks.
 * @author Bela Ban 1999
 * @version $Revision: 1.9 $
 */
public class FD_PROB extends Protocol implements Runnable {
    Address local_addr=null;
    Thread hb=null; // GemStoneAddition synchronize on this to access
    long timeout=3000;  // before a member with a non updated timestamp is suspected
    long gossip_interval=1000;
    Vector members=null;
    final Hashtable counters=new Hashtable();        // keys=Addresses, vals=FdEntries
    final Hashtable invalid_pingers=new Hashtable(); // keys=Address, vals=Integer (number of pings from suspected mbrs)
    int max_tries=2;   // number of times to send a are-you-alive msg (tot time= max_tries*timeout)


    @Override // GemStoneAddition  
    public String getName() {
        return "FD_PROB";
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

        str=props.getProperty("gossip_interval");
        if(str != null) {
            gossip_interval=Long.parseLong(str);
            props.remove("gossip_interval");
        }

        str=props.getProperty("max_tries");
        if(str != null) {
            max_tries=Integer.parseInt(str);
            props.remove("max_tries");
        }

        if(props.size() > 0) {
            log.error(ExternalStrings.FD_PROB_FD_PROBSETPROPERTIES_THE_FOLLOWING_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);

            return false;
        }
        return true;
    }


    @Override // GemStoneAddition  
    public void start() throws Exception {
      synchronized (this) { // GemStoneAddition
        if(hb == null) {
            hb=new Thread(this, "FD_PROB.HeartbeatThread");
            hb.setDaemon(true);
            hb.start();
        }
      }
    }


    @Override // GemStoneAddition  
    public void stop() {
        Thread tmp=null;
        synchronized (this) { // GemStoneAddition
          tmp = hb;
          hb = null;
        }
        if(tmp != null && tmp.isAlive()) {
//            tmp=hb;
//            hb=null; GemStoneAddition
            tmp.interrupt();
            try {
                tmp.join(timeout);
            }
            catch(InterruptedException ex) {
              Thread.currentThread().interrupt(); // GemStoneAddition
              // just propagate to caller
            }
        }
        hb=null;
    }


    @Override // GemStoneAddition  
    public void up(Event evt) {
        Message msg;
//        Address hb_sender; GemStoneAddition
        FdHeader hdr=null;
        Object obj;

        switch(evt.getType()) {

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address) evt.getArg();
                break;

            case Event.MSG:
                msg=(Message) evt.getArg();
                obj=msg.getHeader(getName());
                if(obj == null || !(obj instanceof FdHeader)) {
                    updateCounter(msg.getSrc());  // got a msg from this guy, reset its time (we heard from it now)
                    break;
                }

                hdr=(FdHeader) msg.removeHeader(getName());
                switch(hdr.type) {
                    case FdHeader.HEARTBEAT:                           // heartbeat request; send heartbeat ack
                        if(checkPingerValidity(msg.getSrc()) == false) // false == sender of heartbeat is not a member
                            return;

                        // 2. Update my own array of counters

                            if(log.isInfoEnabled()) log.info(ExternalStrings.FD_PROB__HEARTBEAT_FROM__0, msg.getSrc());
                        updateCounters(hdr);
                        return;                                     // don't pass up !
                    case FdHeader.NOT_MEMBER:
                        if(warn) log.warn("NOT_MEMBER: I'm being shunned; exiting");
                        passUp(new Event(Event.EXIT));
                        return;
                    default:
                        if(warn) log.warn("FdHeader type " + hdr.type + " not known");
                        return;
                }
        }
        passUp(evt);                                        // pass up to the layer above us
    }


    @Override // GemStoneAddition  
    public void down(Event evt) {
//        Message msg; GemStoneAddition
        int num_mbrs;
        Vector excluded_mbrs;
        FdEntry entry;
        Address mbr;

        switch(evt.getType()) {

            // Start heartbeat thread when we have more than 1 member; stop it when membership drops below 2
            case Event.VIEW_CHANGE:
                passDown(evt);
                synchronized(this) {
                    View v=(View) evt.getArg();

                    // mark excluded members
                    excluded_mbrs=computeExcludedMembers(members, v.getMembers());
                    if(excluded_mbrs != null && excluded_mbrs.size() > 0) {
                        for(int i=0; i < excluded_mbrs.size(); i++) {
                            mbr=(Address) excluded_mbrs.elementAt(i);
                            entry=(FdEntry) counters.get(mbr);
                            if(entry != null)
                                entry.setExcluded(true);
                        }
                    }

                    members=v.getMembers(); // GemStoneAddition (cannot be null) v != null ? v.getMembers() : null;
                    if(members != null) {
                        num_mbrs=members.size();
                        if(num_mbrs >= 2) {
                            if(hb == null) {
                                try {
                                    start();
                                }
                                catch(Exception ex) {
                                    if(warn) log.warn("exception when calling start(): " + ex);
                                }
                            }
                        }
                        else
                            stop();
                    }
                }
                break;

            default:
                passDown(evt);
                break;
        }
    }


    /**
     Loop while more than 1 member available. Choose a member randomly (not myself !) and send a
     heartbeat. Wait for ack. If ack not received withing timeout, mcast SUSPECT message.
     */
    public void run() {
        Message hb_msg;
        FdHeader hdr;
        Address hb_dest, key;
        FdEntry entry;
        long curr_time, diff;



            if(log.isInfoEnabled()) log.info(ExternalStrings.FD_PROB_HEARTBEAT_THREAD_WAS_STARTED);

        for (;;) { // GemStoneAddition remove coding anti-pattern
          if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition -- for safety

            // 1. Get a random member P (excluding ourself)
            hb_dest=getHeartbeatDest();
            if(hb_dest == null) {
                if(warn) log.warn("hb_dest is null");
                try { // GemStoneAddition
                  Util.sleep(gossip_interval);
                }
                catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break; // exit the loop (and the thread)
                }
                continue;
            }


            // 2. Increment own counter
            entry=(FdEntry) counters.get(local_addr);
            if(entry == null) {
                entry=new FdEntry();
                counters.put(local_addr, entry);
            }
            entry.incrementCounter();


            // 3. Send heartbeat to P
            hdr=createHeader();
            if(hdr == null)
                if(warn) log.warn("header could not be created. Heartbeat will not be sent");
            else {
                hb_msg=new Message(hb_dest, null, null);
                hb_msg.putHeader(getName(), hdr);

                    if(log.isInfoEnabled()) log.info(ExternalStrings.FD_PROB__HEARTBEAT_TO__0, hb_dest);
                passDown(new Event(Event.MSG, hb_msg));
            }


                if(log.isInfoEnabled()) log.info(ExternalStrings.FD_PROB_OWN_COUNTERS_ARE__0, printCounters());


            // 4. Suspect members from which we haven't heard for timeout msecs
            for(Enumeration e=counters.keys(); e.hasMoreElements();) {
                curr_time=System.currentTimeMillis();
                key=(Address) e.nextElement();
                entry=(FdEntry) counters.get(key);

                if(entry.getTimestamp() > 0 && (diff=curr_time - entry.getTimestamp()) >= timeout) {
                    if(entry.excluded()) {
                        if(diff >= 2 * timeout) {  // remove members marked as 'excluded' after 2*timeout msecs
                            counters.remove(key);
                            if(log.isInfoEnabled()) log.info(ExternalStrings.FD_PROB_REMOVED__0, key);
                        }
                    }
                    else {
                        if(log.isInfoEnabled()) log.info(ExternalStrings.FD_PROB_SUSPECTING__0, key);
                        passUp(new Event(Event.SUSPECT, new SuspectMember(this.local_addr, key))); // GemStoneAddition suspectmember struct
                    }
                }
            }
            try { // GemStoneAddition
              Util.sleep(gossip_interval);
            }
            catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              break; // exit the loop (and the thread)
            }
        } // end while


            if(log.isInfoEnabled()) log.info(ExternalStrings.FD_PROB_HEARTBEAT_THREAD_WAS_STOPPED);
    }







    /* -------------------------------- Private Methods ------------------------------- */

    Address getHeartbeatDest() {
        Address retval=null;
        int r, size;
        Vector members_copy;

        if(members == null || members.size() < 2 || local_addr == null)
            return null;
        members_copy=(Vector) members.clone();
        members_copy.removeElement(local_addr); // don't select myself as heartbeat destination
        size=members_copy.size();
        r=((int) (Math.random() * (size + 1))) % size;
        retval=(Address) members_copy.elementAt(r);
        return retval;
    }


    /** Create a header containing the counters for all members */
    FdHeader createHeader() {
        int num_mbrs=counters.size(), index=0;
        FdHeader ret=null;
        Address key;
        FdEntry entry;

        if(num_mbrs <= 0)
            return null;
        ret=new FdHeader(FdHeader.HEARTBEAT, num_mbrs);
        for(Enumeration e=counters.keys(); e.hasMoreElements();) {
            key=(Address) e.nextElement();
            entry=(FdEntry) counters.get(key);
            if(entry.excluded())
                continue;
            if(index >= ret.members.length) {
                if(warn) log.warn("index " + index + " is out of bounds (" +
                                                     ret.members.length + ')');
                break;
            }
            ret.members[index]=key;
            ret.counters[index]=entry.getCounter();
            index++;
        }
        return ret;
    }


    /** Set my own counters values to max(own-counter, counter) */
    void updateCounters(FdHeader hdr) {
        Address key;
//        long counter; GemStoneAddition
        FdEntry entry;

        if(hdr == null || hdr.members == null || hdr.counters == null) {
            if(warn) log.warn("hdr is null or contains no counters");
            return;
        }

        for(int i=0; i < hdr.members.length; i++) {
            key=hdr.members[i];
            if(key == null) continue;
            entry=(FdEntry) counters.get(key);
            if(entry == null) {
                entry=new FdEntry(hdr.counters[i]);
                counters.put(key, entry);
                continue;
            }

            if(entry.excluded())
                continue;

            // only update counter (and adjust timestamp) if new counter is greater then old one
            entry.setCounter(Math.max(entry.getCounter(), hdr.counters[i]));
        }
    }


    /** Resets the counter for mbr */
    void updateCounter(Address mbr) {
        FdEntry entry;

        if(mbr == null) return;
        entry=(FdEntry) counters.get(mbr);
        if(entry != null)
            entry.setTimestamp();
    }


    String printCounters() {
        StringBuffer sb=new StringBuffer();
        Address mbr;
        FdEntry entry;

        for(Enumeration e=counters.keys(); e.hasMoreElements();) {
            mbr=(Address) e.nextElement();
            entry=(FdEntry) counters.get(mbr);
            sb.append("\n" + mbr + ": " + entry._toString());
        }
        return sb.toString();
    }


    static Vector computeExcludedMembers(Vector old_mbrship, Vector new_mbrship) {
        Vector ret=new Vector();
        if(old_mbrship == null || new_mbrship == null) return ret;
        for(int i=0; i < old_mbrship.size(); i++)
            if(!new_mbrship.contains(old_mbrship.elementAt(i)))
                ret.addElement(old_mbrship.elementAt(i));
        return ret;
    }


    /** If hb_sender is not a member, send a SUSPECT to sender (after n pings received) */
    boolean checkPingerValidity(Object hb_sender) {
        int num_pings=0;
        Message shun_msg;
        Header hdr;

        if(hb_sender != null && members != null && !members.contains(hb_sender)) {
            if(invalid_pingers.containsKey(hb_sender)) {
                num_pings=((Integer) invalid_pingers.get(hb_sender)).intValue();
                if(num_pings >= max_tries) {
                    if(log.isErrorEnabled()) log.error("sender " + hb_sender +
                                                                  " is not member in " + members + " ! Telling it to leave group");
                    shun_msg=new Message((Address) hb_sender, null, null);
                    hdr=new FdHeader(FdHeader.NOT_MEMBER);
                    shun_msg.putHeader(getName(), hdr);
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
            return false;
        }
        else
            return true;
    }


    /* ----------------------------- End of Private Methods --------------------------- */






    public static class FdHeader extends Header  {
        static final int HEARTBEAT=1;  // sent periodically to a random member
        static final int NOT_MEMBER=2;  // sent to the sender, when it is not a member anymore (shunned)


        int type=HEARTBEAT;
        Address[] members=null;
        long[] counters=null;  // correlates with 'members' (same indexes)


        public FdHeader() {
        } // used for externalization

        FdHeader(int type) {
            this.type=type;
        }

        FdHeader(int type, int num_elements) {
            this(type);
            members=new Address[num_elements];
            counters=new long[num_elements];
        }


        @Override // GemStoneAddition  
        public String toString() {
            switch(type) {
                case HEARTBEAT:
                    return "[FD_PROB: HEARTBEAT]";
                case NOT_MEMBER:
                    return "[FD_PROB: NOT_MEMBER]";
                default:
                    return "[FD_PROB: unknown type (" + type + ")]";
            }
        }

        public String printDetails() {
            StringBuffer sb=new StringBuffer();
            Address mbr;
//            long c; GemStoneAddition

            if(members != null && counters != null)
                for(int i=0; i < members.length; i++) {
                    mbr=members[i];
                    if(mbr == null)
                        sb.append("\n<null>");
                    else
                        sb.append("\n" + mbr);
                    sb.append(": " + counters[i]);
                }
            return sb.toString();
        }


        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(type);

            if(members != null) {
                out.writeInt(members.length);
                out.writeObject(members);
            }
            else
                out.writeInt(0);

            if(counters != null) {
                out.writeInt(counters.length);
                for(int i=0; i < counters.length; i++)
                    out.writeLong(counters[i]);
            }
            else
                out.writeInt(0);
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            int num;
            type=in.readInt();

            num=in.readInt();
            if(num == 0)
                members=null;
            else {
                members=(Address[]) in.readObject();
            }

            num=in.readInt();
            if(num == 0)
                counters=null;
            else {
                counters=new long[num];
                for(int i=0; i < counters.length; i++)
                    counters[i]=in.readLong();
            }
        }


    }


    private static class FdEntry  {
        private long counter=0;       // heartbeat counter
        private long timestamp=0;     // last time the counter was incremented
        private boolean excluded=false;  // set to true if member was excluded from group


        FdEntry() {

        }

        FdEntry(long counter) {
            this.counter=counter;
            timestamp=System.currentTimeMillis();
        }


        long getCounter() {
            return counter;
        }

        long getTimestamp() {
            return timestamp;
        }

        boolean excluded() {
            return excluded;
        }


        synchronized void setCounter(long new_counter) {
            if(new_counter > counter) { // only set time if counter was incremented
                timestamp=System.currentTimeMillis();
                counter=new_counter;
            }
        }

        synchronized void incrementCounter() {
            counter++;
            timestamp=System.currentTimeMillis();
        }

        synchronized void setTimestamp() {
            timestamp=System.currentTimeMillis();
        }

        synchronized void setExcluded(boolean flag) {
            excluded=flag;
        }


        @Override // GemStoneAddition  
        public String toString() {
            return "counter=" + counter + ", timestamp=" + timestamp + ", excluded=" + excluded;
        }

        public String _toString() {
            return "counter=" + counter + ", age=" + (System.currentTimeMillis() - timestamp) +
                    ", excluded=" + excluded;
        }
    }


}
