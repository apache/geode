/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: STABLE.java,v 1.39 2005/08/11 12:43:46 belaban Exp $

package com.gemstone.org.jgroups.protocols.pbcast;



import com.gemstone.org.jgroups.*;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Streamable;
import com.gemstone.org.jgroups.util.TimeScheduler;
import com.gemstone.org.jgroups.util.Util;

import java.io.*;
import java.util.*;




/**
 * Computes the broadcast messages that are stable; i.e., have been received by all members. Sends
 * STABLE events up the stack when this is the case. This allows NAKACK to garbage collect messages that
 * have been seen by all members.<p>
 * Works as follows: periodically we mcast our highest seqnos (seen for each member) to the group.
 * A stability vector, which maintains the highest seqno for each member and initially contains no data,
 * is updated when such a message is received. The entry for a member P is computed set to
 * min(entry[P], digest[P]). When messages from all members have been received, a stability
 * message is mcast, which causes all members to send a STABLE event up the stack (triggering garbage collection
 * in the NAKACK layer).<p>
 * The stable task now terminates after max_num_gossips if no messages or view changes have been sent or received
 * in the meantime. It will resume when messages are received. This effectively suspends sending superfluous
 * STABLE messages in the face of no activity.<br/>
 * New: when <code>max_bytes</code> is exceeded (unless disabled by setting it to 0),
 * a STABLE task will be started (unless it is already running).
 * @author Bela Ban
 */
public class STABLE extends Protocol  {
    Address             local_addr=null;
    final Vector        mbrs=new Vector();
    final Digest        digest=new Digest(10);        // keeps track of the highest seqnos from all members
    final Digest        latest_local_digest=new Digest(10); // keeps track of the latest digests received from NAKACK
    final Vector        heard_from=new Vector();      // keeps track of who we already heard from (STABLE_GOSSIP msgs)

    /** Sends a STABLE gossip every 20 seconds on average. 0 disables gossipping of STABLE messages */
    long                desired_avg_gossip=20000;

    /** delay before we send STABILITY msg (give others a change to send first). This should be set to a very
     * small number (> 0 !) if <code>max_bytes</code> is used */
    long                stability_delay=6000;
    StabilitySendTask   stability_task=null;
    final Object        stability_mutex=new Object(); // to synchronize on stability_task
    volatile/*GemStoneAddition*/ StableTask          stable_task=null;             // bcasts periodic STABLE message (added to timer below)
    final Object        stable_task_mutex=new Object(); // to sync on stable_task
    TimeScheduler       timer=null;                   // to send periodic STABLE msgs (and STABILITY messages)
    static final String name="STABLE";

    /** Total amount of bytes from incoming messages (default = 0 = disabled). When exceeded, a STABLE
     * message will be broadcast and <code>num_bytes_received</code> reset to 0 . If this is > 0, then ideally
     * <code>stability_delay</code> should be set to a low number as well */
    long                max_bytes=0;

    /** The total number of bytes received from unicast and multicast messages */
    long                num_bytes_received=0;

    /** When true, don't take part in garbage collection protocol: neither send STABLE messages nor
     * handle STABILITY messages */
    boolean             suspended=false;

    boolean             initialized=false;

    ResumeTask          resume_task=null;
    final Object        resume_task_mutex=new Object();
    private long suspendStartTime = 0;

    /** Number of gossip messages */
    int                 num_gossips=0;



    @Override // GemStoneAddition
    public String getName() {
        return name;
    }

    // start GemStoneAddition
    @Override // GemStoneAddition
    public int getProtocolEnum() {
      return com.gemstone.org.jgroups.stack.Protocol.enumSTABLE;
    }
    // end GemStone addition


    public long getDesiredAverageGossip() {
        return desired_avg_gossip;
    }

    public void setDesiredAverageGossip(long gossip_interval) {
        desired_avg_gossip=gossip_interval;
    }

    public long getMaxBytes() {
        return max_bytes;
    }

    public void setMaxBytes(long max_bytes) {
        this.max_bytes=max_bytes;
    }

    public int getNumberOfGossipMessages() {return num_gossips;}

    @Override // GemStoneAddition
    public void resetStats() {
        super.resetStats();
        num_gossips=0;
    }


    @Override // GemStoneAddition
    public Vector requiredDownServices() {
        Vector retval=new Vector();
        retval.addElement(Integer.valueOf(Event.GET_DIGEST_STABLE));  // NAKACK layer
        return retval;
    }

    @Override // GemStoneAddition
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("digest_timeout");
        if(str != null) {
            props.remove("digest_timeout");
            log.error(ExternalStrings.STABLE_DIGEST_TIMEOUT_HAS_BEEN_DEPRECATED_IT_WILL_BE_IGNORED);
        }

        str=props.getProperty("desired_avg_gossip");
        if(str != null) {
            desired_avg_gossip=Long.parseLong(str);
            props.remove("desired_avg_gossip");
        }

        str=props.getProperty("stability_delay");
        if(str != null) {
            stability_delay=Long.parseLong(str);
            props.remove("stability_delay");
        }

        str=props.getProperty("max_gossip_runs");
        if(str != null) {
            props.remove("max_gossip_runs");
            log.error(ExternalStrings.STABLE_MAX_GOSSIP_RUNS_HAS_BEEN_DEPRECATED_AND_WILL_BE_IGNORED);
        }

        str=props.getProperty("max_bytes");
        if(str != null) {
            max_bytes=Long.parseLong(str);
            props.remove("max_bytes");
        }

        str=props.getProperty("max_suspend_time");
        if(str != null) {
            log.error(ExternalStrings.STABLE_MAX_SUSPEND_TIME_IS_NOT_SUPPORTED_ANY_LONGER_PLEASE_REMOVE_IT_IGNORING_IT);
            props.remove("max_suspend_time");
        }

        if(props.size() > 0) {
            log.error(ExternalStrings.STABLE_THESE_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);
            
            return false;
        }
        return true;
    }


    private void suspend(long timeout) {
        if(!suspended) {
            suspended=true;
            if(log.isDebugEnabled())
                log.debug("suspending message garbage collection");
            if (stack.enableClockStats)
              suspendStartTime = nanoTime(); // GemStoneAddition
        }
        startResumeTask(timeout); // will not start task if already running
    }

    protected/*GemStoneAddition*/ void resume() {
        suspended=false;
        if(log.isDebugEnabled())
            log.debug("resuming message garbage collection");
        stopResumeTask();
    }

    @Override // GemStoneAddition
    public void start() throws Exception {
        if(stack != null && stack.timer != null)
            timer=stack.timer;
        else
            throw new Exception("timer cannot be retrieved from protocol stack");
        if(desired_avg_gossip > 0)
            startStableTask();
    }

    @Override // GemStoneAddition
    public void stop() {
        stopStableTask();
        clearDigest();
    }


    @Override // GemStoneAddition
    public void up(Event evt) {
        Message msg;
        StableHeader hdr;
        int type=evt.getType();
//        long start; // GemStoneAddition

        switch(type) {

        case Event.MSG:
            msg=(Message)evt.getArg();
            if(max_bytes > 0) {  // message counting is enabled
                long size=Math.max(msg.getLength(), 24);
                num_bytes_received+=size;
                if(num_bytes_received >= max_bytes) {
                    if(trace) {
                        log.trace(new StringBuffer("max_bytes has been reached (").append(max_bytes).
                                  append(", bytes received=").append(num_bytes_received).append("): triggers stable msg"));
                    }
                    num_bytes_received=0;
                    // asks the NAKACK protocol for the current digest, reply event is GET_DIGEST_STABLE_OK (arg=digest)
                    passDown(new Event(Event.GET_DIGEST_STABLE));
                }
            }

            hdr=(StableHeader)msg.removeHeader(name);
            if(hdr == null)
                break;
            switch(hdr.type) {
            case StableHeader.STABLE_GOSSIP:
                handleStableMessage(msg.getSrc(), hdr.stableDigest);
                break;
            case StableHeader.STABILITY:
                handleStabilityMessage(hdr.stableDigest, msg.getSrc());
                break;
            default:
                if(log.isErrorEnabled()) log.error(ExternalStrings.STABLE_STABLEHEADER_TYPE__0__NOT_KNOWN, hdr.type);
            }
            return;  // don't pass STABLE or STABILITY messages up the stack

        case Event.GET_DIGEST_STABLE_OK:
            Digest d=(Digest)evt.getArg();
            synchronized(latest_local_digest) {
                latest_local_digest.replace(d);
            }
            if(trace)
                log.trace("setting latest_local_digest from NAKACK: " + d.printHighSeqnos());
            sendStableMessage(d);
            break;

        case Event.VIEW_CHANGE:
            View view=(View)evt.getArg();
            handleViewChange(view);
            break;

        case Event.SET_LOCAL_ADDRESS:
            local_addr=(Address)evt.getArg();
            break;
        }
        passUp(evt);
    }




    @Override // GemStoneAddition
    public void down(Event evt) {
        switch(evt.getType()) {
        case Event.VIEW_CHANGE:
            View v=(View)evt.getArg();
            handleViewChange(v);
            break;

        case Event.SUSPEND_STABLE:
            long timeout=0;
            Object t=evt.getArg();
            if(t != null && t instanceof Long)
                timeout=((Long)t).longValue();
            suspend(timeout);
            break;

        case Event.RESUME_STABLE:
            resume();
            break;
        }
        passDown(evt);
    }


    public void runMessageGarbageCollection() {
        Digest copy;
        synchronized(digest) {
            copy=digest.copy();
        }
        sendStableMessage(copy);
    }



    /* --------------------------------------- Private Methods ---------------------------------------- */


    private void handleViewChange(View v) {
        Vector tmp=v.getMembers();
        mbrs.clear();
        mbrs.addAll(tmp);
        adjustSenders(digest, tmp);
        adjustSenders(latest_local_digest, tmp);
        resetDigest(tmp);
        if(!initialized)
            initialized=true;
    }


    /** Digest and members are guaranteed to be non-null */
    private void adjustSenders(Digest d, Vector members) {
        synchronized(d) {
            // 1. remove all members from digest who are not in the view
            Iterator it=d.senders.keySet().iterator();
            Address mbr;
            while(it.hasNext()) {
                mbr=(Address)it.next();
                if(!members.contains(mbr))
                    it.remove();
            }
            // 2. add members to digest which are in the new view but not in the digest
            for(int i=0; i < members.size(); i++) {
                mbr=(Address)members.get(i);
                if(!d.contains(mbr))
                    d.add(mbr, -1, -1);
            }
        }
    }


    private void clearDigest() {
        synchronized(digest) {
            digest.clear();
        }
    }



    /** Update my own digest from a digest received by somebody else. Returns whether the update was successful.
     *  Needs to be called with a lock on digest */
    private boolean updateLocalDigest(Digest d, Address sender) {
        if(d == null || d.size() == 0)
            return false;

        if(!initialized) {
            if(trace)
                log.trace("STABLE message will not be handled as I'm not yet initialized");
            return false;
        }

        if(!digest.sameSenders(d)) {
            if(trace)
                log.trace(new StringBuffer("received a digest ").append(d.printHighSeqnos()).append(" from ").
                          append(sender).append(" which has different members than mine (").
                          append(digest.printHighSeqnos()).append("), discarding it and resetting heard_from list"));
            // to avoid sending incorrect stability/stable msgs, we simply reset our heard_from list, see DESIGN
            resetDigest(mbrs);
            return false;
        }

        StringBuffer sb=null;
        if(trace)
            sb=new StringBuffer("my [").append(local_addr).append("] digest before: ").append(digest).
                    append("\ndigest from ").append(sender).append(": ").append(d);
        Address mbr;
        long highest_seqno, my_highest_seqno, new_highest_seqno;
        long highest_seen_seqno, my_highest_seen_seqno, new_highest_seen_seqno;
        Map.Entry entry;
        com.gemstone.org.jgroups.protocols.pbcast.Digest.Entry val;
        for(Iterator it=d.senders.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            mbr=(Address)entry.getKey();
            val=(com.gemstone.org.jgroups.protocols.pbcast.Digest.Entry)entry.getValue();
            highest_seqno=val.high_seqno;
            highest_seen_seqno=val.high_seqno_seen;

            // compute the minimum of the highest seqnos deliverable (for garbage collection)
            my_highest_seqno=digest.highSeqnoAt(mbr);
            // compute the maximum of the highest seqnos seen (for retransmission of last missing message)
            my_highest_seen_seqno=digest.highSeqnoSeenAt(mbr);

            new_highest_seqno=Math.min(my_highest_seqno, highest_seqno);
            new_highest_seen_seqno=Math.max(my_highest_seen_seqno, highest_seen_seqno);
            digest.setHighestDeliveredAndSeenSeqnos(mbr, new_highest_seqno, new_highest_seen_seqno);
        }
        if(trace) {
            sb.append("\nmy [").append(local_addr).append("] digest after: ").append(digest).append("\n");
            log.trace(sb);
        }
        return true;
    }



    private void resetDigest(Vector new_members) {
        if(new_members == null || new_members.size() == 0)
            return;
        synchronized(heard_from) {
            heard_from.clear();
            heard_from.addAll(new_members);
        }

        Digest copy_of_latest;
        synchronized(latest_local_digest) {
            copy_of_latest=latest_local_digest.copy();
        }
        synchronized(digest) {
            digest.replace(copy_of_latest);
            if(trace)
                log.trace("resetting digest from NAKACK: " + copy_of_latest.printHighSeqnos());
        }
    }

    /**
     * Removes mbr from heard_from and returns true if this was the last member, otherwise false.
     * Resets the heard_from list (populates with membership)
     * @param mbr
     * @return boolean
     */
    private boolean removeFromHeardFromList(Address mbr) {
        synchronized(heard_from) {
            heard_from.remove(mbr);
            if(heard_from.size() == 0) {
                resetDigest(this.mbrs);
                return true;
            }
        }
        return false;
    }


    void startStableTask() {
        // Here, double-checked locking works: we don't want to synchronize if the task already runs (which is the case
        // 99% of the time). If stable_task gets nulled after the condition check, we return anyways, but just miss
        // 1 cycle: on the next message or view, we will start the task
        if(stable_task != null)
            return;
        synchronized(stable_task_mutex) {
            if(stable_task != null && stable_task.running()) {
                return;  // already running
            }
            stable_task=new StableTask();
            timer.add(stable_task, true); // fixed-rate scheduling
        }
        if(trace)
            log.trace("stable task started");
    }


    void stopStableTask() {
        // contrary to startStableTask(), we don't need double-checked locking here because this method is not
        // called frequently
        synchronized(stable_task_mutex) {
            if(stable_task != null) {
                stable_task.stop();
                stable_task=null;
            }
        }
    }


    void startResumeTask(long max_suspend_time) {
        max_suspend_time=(long)(max_suspend_time * 1.1); // little slack

        synchronized(resume_task_mutex) {
            if(resume_task != null && resume_task.running()) {
                return;  // already running
            }
            else {
                resume_task=new ResumeTask(max_suspend_time);
                timer.add(resume_task, true); // fixed-rate scheduling
            }
        }
        if(log.isDebugEnabled())
            log.debug("resume task started, max_suspend_time=" + max_suspend_time);
    }


    void stopResumeTask() {
        synchronized(resume_task_mutex) {
            if(resume_task != null) {
                resume_task.stop();
                resume_task=null;
                if (stack.enableClockStats)
                  stack.gfPeerFunctions.incJgSTABLEsuspendTime(nanoTime() - suspendStartTime);
            }
        }
    }


    void startStabilityTask(Digest d, long delay) {
        synchronized(stability_mutex) {
            if(stability_task != null && stability_task.running()) {
            }
            else {
                stability_task=new StabilitySendTask(d, delay); // runs only once
                timer.add(stability_task, true);
            }
        }
    }


    void stopStabilityTask() {
        synchronized(stability_mutex) {
            if(stability_task != null) {
                stability_task.stop();
                stability_task=null;
            }
        }
    }


    /**
     Digest d contains (a) the highest seqnos <em>deliverable</em> for each sender and (b) the highest seqnos
     <em>seen</em> for each member. (Difference: with 1,2,4,5, the highest seqno seen is 5, whereas the highest
     seqno deliverable is 2). The minimum of all highest seqnos deliverable will be taken to send a stability
     message, which results in garbage collection of messages lower than the ones in the stability vector. The
     maximum of all seqnos will be taken to trigger possible retransmission of last missing seqno (see DESIGN
     for details).
     */
    private void handleStableMessage(Address sender, Digest d) {
        if(d == null || sender == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.STABLE_DIGEST_OR_SENDER_IS_NULL);
            return;
        }

        if(!initialized) {
            if(trace)
                log.trace("STABLE message will not be handled as I'm not yet initialized");
            return;
        }

        if(suspended) {
            if(trace)
                log.trace("STABLE message will not be handled as I'm suspended");
            return;
        }

        if(trace)
            log.trace(new StringBuffer("received stable msg from ").append(sender).append(": ").append(d.printHighSeqnos()));
        if(!heard_from.contains(sender)) {  // already received gossip from sender; discard it
            if(trace) log.trace("already received stable msg from " + sender);
            return;
        }
        
        stack.gfPeerFunctions.incJgSTABLEmessages(1);
        
        Digest copy;
        synchronized(digest) {
            boolean success=updateLocalDigest(d, sender);
            if(!success) // we can only remove the sender from heard_from if *all* elements of my digest were updated
                return;
            copy=digest.copy();
        }

        boolean was_last=removeFromHeardFromList(sender);
        if(was_last) {
            sendStabilityMessage(copy);
        }
    }


    /**
     * Bcasts a STABLE message of the current digest to all members. Message contains highest seqnos of all members
     * seen by this member. Highest seqnos are retrieved from the NAKACK layer below.
     * @param d A <em>copy</em> of this.digest
     */
    private void sendStableMessage(Digest d) {
        if(suspended) {
            if(trace)
                log.trace("will not send STABLE message as I'm suspended");
            return;
        }
        stack.gfPeerFunctions.incJgSTABLEmessagesSent(1);

        if(d != null && d.size() > 0) {
            if(trace)
                log.trace("sending stable msg " + d.printHighSeqnos());
            Message msg=new Message(); // mcast message
            StableHeader hdr=new StableHeader(StableHeader.STABLE_GOSSIP, d);
            msg.putHeader(name, hdr);
            num_gossips++;
            passDown(new Event(Event.MSG, msg));
        }
    }



    /**
     Schedules a stability message to be mcast after a random number of milliseconds (range 1-5 secs).
     The reason for waiting a random amount of time is that, in the worst case, all members receive a
     STABLE_GOSSIP message from the last outstanding member at the same time and would therefore mcast the
     STABILITY message at the same time too. To avoid this, each member waits random N msecs. If, before N
     elapses, some other member sent the STABILITY message, we just cancel our own message. If, during
     waiting for N msecs to send STABILITY message S1, another STABILITY message S2 is to be sent, we just
     discard S2.
     @param tmp A copy of te stability digest, so we don't need to copy it again
     */
    void sendStabilityMessage(Digest tmp) {
        long delay;

        // give other members a chance to mcast STABILITY message. if we receive STABILITY by the end of
        // our random sleep, we will not send the STABILITY msg. this prevents that all mbrs mcast a
        // STABILITY msg at the same time
        delay=Util.random(stability_delay);
        startStabilityTask(tmp, delay);
    }


    void handleStabilityMessage(Digest d, Address sender) {
        if(d == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.STABLE_STABILITY_DIGEST_IS_NULL);
            return;
        }

        if(!initialized) {
            if(trace)
                log.trace("STABLE message will not be handled as I'm not yet initialized");
            return;
        }

        if(suspended) {
            if(log.isDebugEnabled()) {
                log.debug("stability message will not be handled as I'm suspended");
            }
            return;
        }

        if(trace)
            log.trace(new StringBuffer("received stability msg from ").append(sender).append(": ").append(d.printHighSeqnos()));
        stopStabilityTask();
        stack.gfPeerFunctions.incJgSTABILITYmessages(1);

        // we won't handle the gossip d, if d's members don't match the membership in my own digest,
        // this is part of the fix for the NAKACK problem (bugs #943480 and #938584)
        if(!this.digest.sameSenders(d)) {
            if(log.isDebugEnabled()) {
                log.debug("received digest (digest=" + d + ") which does not match my own digest ("+
                        this.digest + "): ignoring digest and re-initializing own digest");
            }
            return;
        }

        resetDigest(mbrs);

        // pass STABLE event down the stack, so NAKACK can garbage collect old messages
        passDown(new Event(Event.STABLE, d));
    }



    /* ------------------------------------End of Private Methods ------------------------------------- */







    public static class StableHeader extends Header implements Streamable {
        public static final int STABLE_GOSSIP=1;
        public static final int STABILITY=2;

        int type=0;
        // Digest digest=new Digest();  // used for both STABLE_GOSSIP and STABILITY message
        Digest stableDigest=null; // changed by Bela April 4 2004

        public StableHeader() {
        } // used for externalizable


        public StableHeader(int type, Digest digest) {
            this.type=type;
            this.stableDigest=digest;
        }


        static String type2String(int t) {
            switch(t) {
                case STABLE_GOSSIP:
                    return "STABLE_GOSSIP";
                case STABILITY:
                    return "STABILITY";
                default:
                    return "<unknown>";
            }
        }

        @Override // GemStoneAddition
        public String toString() {
            StringBuffer sb=new StringBuffer();
            sb.append('[');
            sb.append(type2String(type));
            sb.append("]: digest is ");
            sb.append(stableDigest);
            return sb.toString();
        }


        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(type);
            if(stableDigest == null) {
                out.writeBoolean(false);
                return;
            }
            out.writeBoolean(true);
            stableDigest.writeExternal(out);
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readInt();
            boolean digest_not_null=in.readBoolean();
            if(digest_not_null) {
                stableDigest=new Digest();
                stableDigest.readExternal(in);
            }
        }

        @Override // GemStoneAddition
        public long size(short version) {
            long retval=Global.INT_SIZE + Global.BYTE_SIZE; // type + presence for digest
            if(stableDigest != null)
                retval+=stableDigest.serializedSize(version);
            return retval;
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeInt(type);
            Util.writeStreamable(stableDigest, out);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readInt();
            stableDigest=(Digest)Util.readStreamable(Digest.class, in);
        }


    }




    /**
     Mcast periodic STABLE message. Interval between sends varies. Terminates after num_gossip_runs is 0.
     However, UP or DOWN messages will reset num_gossip_runs to max_gossip_runs. This has the effect that the
     stable_send task terminates only after a period of time within which no messages were either sent
     or received
     */
    protected/*GemStoneAddition*/ class StableTask implements TimeScheduler.Task {
        boolean stopped=false;

        public void stop() {
            stopped=true;
        }

        public boolean running() { // syntactic sugar
            return !stopped;
        }

        public boolean cancelled() {
            return stopped;
        }

        public long nextInterval() {
            long interval=computeSleepTime();
            if(interval <= 0)
                return 10000;
            else
                return interval;
        }


        public void run() {
            if(suspended) {
                if(trace)
                    log.trace("stable task will not run as suspended=" + suspended);
                return;
            }

            // asks the NAKACK protocol for the current digest, reply event is GET_DIGEST_STABLE_OK (arg=digest)
            passDown(new Event(Event.GET_DIGEST_STABLE));
        }

        long computeSleepTime() {
            return getRandom((mbrs.size() * desired_avg_gossip * 2));
        }

        long getRandom(long range) {
            return (long)((Math.random() * range) % range);
        }
    }





    /**
     * Multicasts a STABILITY message.
     */
    private class StabilitySendTask implements TimeScheduler.Task {
        Digest   d=null;
        boolean  stopped=false;
        long     delay=2000;


        StabilitySendTask(Digest d, long delay) {
            this.d=d;
            this.delay=delay;
        }

        public boolean running() {
            return !stopped;
        }

        public void stop() {
            stopped=true;
        }

        public boolean cancelled() {
            return stopped;
        }


        /** wait a random number of msecs (to give other a chance to send the STABILITY msg first) */
        public long nextInterval() {
            return delay;
        }


        public void run() {
            Message msg;
            StableHeader hdr;

            if(suspended) {
                if(log.isDebugEnabled()) {
                    log.debug("STABILITY message will not be sent as suspended=" + suspended);
                }
                stopped=true;
                return;
            }

            if(d != null && !stopped) {
                msg=new Message();
                hdr=new StableHeader(StableHeader.STABILITY, d);
                msg.putHeader(STABLE.name, hdr);
                if(trace) log.trace("sending stability msg " + d.printHighSeqnos());
                passDown(new Event(Event.MSG, msg));
                d=null;
            }
            stopped=true; // run only once
        }
    }


    private class ResumeTask implements TimeScheduler.Task {
        boolean running=true;
        long max_suspend_time=0;

        ResumeTask(long max_suspend_time) {
            this.max_suspend_time=max_suspend_time;
        }

        void stop() {
            running=false;
        }

        public boolean running() {
            return running;
        }

        public boolean cancelled() {
            return running == false;
        }

        public long nextInterval() {
            return max_suspend_time;
        }

        public void run() {
            if(suspended)
                log.warn("ResumeTask resumed message garbage collection - this should be done by a RESUME_STABLE event; " +
                         "check why this event was not received (or increase max_suspend_time for large state transfers)");
            resume();
        }
    }


}
