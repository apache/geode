/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: AckReceiverWindow.java,v 1.20 2005/08/26 11:32:44 belaban Exp $

package com.gemstone.org.jgroups.stack;


import com.gemstone.org.jgroups.util.GemFireTracer;


import com.gemstone.org.jgroups.Message;

import java.util.HashMap;
import java.util.TreeSet;


/**
 * Counterpart of AckSenderWindow. Simple FIFO buffer.
 * Every message received is ACK'ed (even duplicates) and added to a hashmap
 * keyed by seqno. The next seqno to be received is stored in <code>next_to_remove</code>. When a message with
 * a seqno less than next_to_remove is received, it will be discarded. The <code>remove()</code> method removes
 * and returns a message whose seqno is equal to next_to_remove, or null if not found.<br>
 * Change May 28 2002 (bela): replaced TreeSet with HashMap. Keys do not need to be sorted, and adding a key to
 * a sorted set incurs overhead.
 *
 * @author Bela Ban
 */
public class AckReceiverWindow {
    long              next_to_remove=0;
    final HashMap     msgs=new HashMap();  // keys: seqnos (Long), values: Messages
    static final GemFireTracer log=GemFireTracer.getLog(AckReceiverWindow.class);
    
    /**
     * GemStoneAddition - for DirAck we must track the highest seqno for which
     * a releasing Ack has been sent
     */
    long highest_released_seqno;
    
    /**
     * GemStoneAddition - track whether a releasing Ack has been sent for
     * a message retrieved from this window
     * 
     * @param seqno the released sequence number
     */
    public synchronized void releasedMessage(long seqno) {
      if (seqno > highest_released_seqno) {
        highest_released_seqno = seqno;
      }
    }
    
    public long getHighestReleasedSeqno() {
      return highest_released_seqno;
    }


    public AckReceiverWindow(long initial_seqno) {
        this.next_to_remove=initial_seqno;
    }


    /** Adds a new message. Message cannot be null */
    public boolean add2(long seqno, Message msg) { // GemStoneAddition - this is add() that returns false if msg not retained
        if(msg == null)
            throw new IllegalArgumentException("msg must be non-null");
        synchronized(msgs) {
            if(seqno < next_to_remove) {
                if(log.isTraceEnabled())
                    // GemStoneAddition - better logging
                    log.trace("discarded msg with seqno=" + seqno + " (next msg to dispatch is " + next_to_remove + ')');
                return false;
            }
            Long seq=Long.valueOf(seqno);
            if(!msgs.containsKey(seq)) { // todo: replace with atomic action once we have util.concurrent (JDK 5)
                msgs.put(seq, msg);
                // GemStoneAddition - better logging
                if (log.isTraceEnabled()) {
                  log.trace("seqno " + seqno + " added to receiver window (next" +
                                " msg to dispatch is " + next_to_remove + ')');
                }
            }
            else {
                if(log.isTraceEnabled())
                  // GemStoneAddition - better logging
                    log.trace("seqno " + seqno + " already received and awaiting" +
                    " dispatch (next msg to dispatch is " + next_to_remove + ')');
            }
            return true;
        }
    }
    
    /** GemStoneAddition - returns the next seqno to dispatch */
    public long nextToRemove() {
      return this.next_to_remove;
    }


    /** Adds a new message. Message cannot be null
     * @return true if the message was added, false if it was already there (GemStoneAddition)
     */
    public boolean add(long seqno, Message msg) { // GemStoneAddition - return false if msg not retained
        if(msg == null)
            throw new IllegalArgumentException("msg must be non-null");
        synchronized(msgs) {
            if(seqno < next_to_remove) {
                if(log.isTraceEnabled())
                    log.trace("discarded msg with seqno=" + seqno + " (next msg to receive is " + next_to_remove + ')');
                return false;
            }
            Long seq=Long.valueOf(seqno);
            if(!msgs.containsKey(seq)) { // todo: replace with atomic action once we have util.concurrent (JDK 5)
                msgs.put(seq, msg);
                return true;
            }
            else {
                if(log.isTraceEnabled())
                    log.trace("seqno " + seqno + " already received - dropping it");
                return false;
            }
        }
    }


    /**
     * Removes a message whose seqno is equal to <code>next_to_remove</code>, increments the latter.
     * Returns message that was removed, or null, if no message can be removed. Messages are thus
     * removed in order.
     */
    public Message remove() {
        Message retval;

        synchronized(msgs) {
            Long key=Long.valueOf(next_to_remove);
            retval=(Message)msgs.remove(key);
            if(retval != null) {
                if(log.isTraceEnabled())
                    log.trace("removed seqno=" + next_to_remove + ": " + retval);
                next_to_remove++;
            }
        }
        return retval;
    }
    
    /** GemStoneAddition - see if the given message is the next to be processed */
    public boolean isNextToRemove(long seqno) {
      synchronized(msgs) {
        return seqno == next_to_remove;
      }
    }


    public void reset() {
        synchronized(msgs) {
            msgs.clear();
        }
    }

    public int size() {
        return msgs.size();
    }

    @Override // GemStoneAddition
    public String toString() {
        StringBuffer sb=new StringBuffer();
        sb.append("(msgs pending=").append(msgs.size()).append(" next=").append(next_to_remove).append(")");
        TreeSet s=new TreeSet(msgs.keySet());
        if(s.size() > 0) {
            sb.append(" [").append(s.first()).append(" - ").append(s.last()).append("]");
            sb.append(": ").append(s);
        }
        return sb.toString();
    }


    public String printDetails() {
        StringBuffer sb=new StringBuffer();
        sb.append(msgs.size()).append(" msgs (").append("next=").append(next_to_remove).append(")").
                append(", msgs=" ).append(new TreeSet(msgs.keySet()));
        return sb.toString();
    }


}
