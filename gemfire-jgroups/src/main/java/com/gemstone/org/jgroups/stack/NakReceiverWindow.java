/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: NakReceiverWindow.java,v 1.27 2005/09/29 08:33:14 belaban Exp $


package com.gemstone.org.jgroups.stack;

import com.gemstone.org.jgroups.oswego.concurrent.ReadWriteLock;
import com.gemstone.org.jgroups.oswego.concurrent.WriterPreferenceReadWriteLock;
import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.List;
import com.gemstone.org.jgroups.util.TimeScheduler;

import java.util.*;



/**
 * Keeps track of messages according to their sequence numbers. Allows
 * messages to be added out of order, and with gaps between sequence numbers.
 * Method <code>remove()</code> removes the first message with a sequence
 * number that is 1 higher than <code>next_to_remove</code> (this variable is
 * then incremented), or it returns null if no message is present, or if no
 * message's sequence number is 1 higher.
 * <p>
 * When there is a gap upon adding a message, its seqno will be added to the
 * Retransmitter, which (using a timer) requests retransmissions of missing
 * messages and keeps on trying until the message has been received, or the
 * member who sent the message is suspected.
 * <p>
 * Started out as a copy of SlidingWindow. Main diff: RetransmitCommand is
 * different, and retransmission thread is only created upon detection of a
 * gap.
 * <p>
 * Change Nov 24 2000 (bela): for PBCAST, which has its own retransmission
 * (via gossip), the retransmitter thread can be turned off
 * <p>
 * Change April 25 2001 (igeorg):<br>
 * i. Restructuring: placed all nested class definitions at the top, then
 * class static/non-static variables, then class private/public methods.<br>
 * ii. Class and all nested classes are thread safe. Readers/writer lock
 * added on <tt>NakReceiverWindow</tt> for finer grained locking.<br>
 * iii. Internal or externally provided retransmission scheduler thread.<br>
 * iv. Exponential backoff in time for retransmissions.<br>
 *
 * @author Bela Ban May 27 1999, May 2004
 * @author John Georgiadis May 8 2001
 */
public class NakReceiverWindow  {

    public interface Listener {
        void missingMessageReceived(long seqno, Message msg);
    }


    /** The big read/write lock */
    private final ReadWriteLock lock=new WriterPreferenceReadWriteLock();
    //private final ReadWriteLock lock=new NullReadWriteLock();

    /** keep track of *next* seqno to remove and highest received */
    private long   head=0;
    private long   tail=0;

    /** lowest seqno delivered so far */
    private long   lowest_seen=0;

    /** highest deliverable (or delivered) seqno so far */
    private long   highest_seen=0;

    /** TreeMap<Long,Message>. Maintains messages keyed by (sorted) sequence numbers */
    private final TreeMap received_msgs=new TreeMap();

    /** TreeMap<Long,Message>. Delivered (= seen by all members) messages. A remove() method causes a message to be
     moved from received_msgs to delivered_msgs. Message garbage collection will gradually remove elements in this map */
    private final TreeMap delivered_msgs=new TreeMap();

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

    /** if not set, no retransmitter thread will be started. Useful if
     * protocols do their own retransmission (e.g PBCAST) */
    private Retransmitter retransmitter=null;

    private Listener listener=null;

    protected static final GemFireTracer log=GemFireTracer.getLog(NakReceiverWindow.class);


    /**
     * Creates a new instance with the given retransmit command
     *
     * @param sender The sender associated with this instance
     * @param cmd The command used to retransmit a missing message, will
     * be invoked by the table. If null, the retransmit thread will not be
     * started
     * @param start_seqno The first sequence number to be received
     * @param sched the external scheduler to use for retransmission
     * requests of missing msgs. If it's not provided or is null, an internal
     * one is created
     */
    public NakReceiverWindow(Address sender, Retransmitter.RetransmitCommand cmd,
                             long start_seqno, TimeScheduler sched) {
        head=start_seqno;
        tail=head;

        if(cmd != null)
            retransmitter=sched == null ?
                    new Retransmitter(sender, cmd) :
                    new Retransmitter(sender, cmd, sched);
    }

    /**
     * Creates a new instance with the given retransmit command
     *
     * @param sender The sender associated with this instance
     * @param cmd The command used to retransmit a missing message, will
     * be invoked by the table. If null, the retransmit thread will not be
     * started
     * @param start_seqno The first sequence number to be received
     */
    public NakReceiverWindow(Address sender, Retransmitter.RetransmitCommand cmd, long start_seqno) {
        this(sender, cmd, start_seqno, null);
    }

    /**
     * Creates a new instance without a retransmission thread
     *
     * @param sender The sender associated with this instance
     * @param start_seqno The first sequence number to be received
     */
    public NakReceiverWindow(Address sender, long start_seqno) {
        this(sender, null, start_seqno);
    }


    public void setRetransmitTimeouts(long[] timeouts) {
        if(retransmitter != null)
            retransmitter.setRetransmitTimeouts(timeouts);
    }


    public void setDiscardDeliveredMessages(boolean flag) {
        this.discard_delivered_msgs=flag;
    }

    public int getMaxXmitBufSize() {
        return max_xmit_buf_size;
    }

    public void setMaxXmitBufSize(int max_xmit_buf_size) {
        this.max_xmit_buf_size=max_xmit_buf_size;
    }

    public void setListener(Listener l) {
        this.listener=l;
    }


    /**
     * Adds a message according to its sequence number (ordered).
     * <p>
     * Variables <code>head</code> and <code>tail</code> mark the start and
     * end of the messages received, but not delivered yet. When a message is
     * received, if its seqno is smaller than <code>head</code>, it is
     * discarded (already received). If it is bigger than <code>tail</code>,
     * we advance <code>tail</code> and add empty elements. If it is between
     * <code>head</code> and <code>tail</code>, we set the corresponding
     * missing (or already present) element. If it is equal to
     * <code>tail</code>, we advance the latter by 1 and add the message
     * (default case).
     */
    public void add(long seqno, Message msg) {
        long old_tail;

        try {
            lock.writeLock().acquire();
            try {
                old_tail=tail;
                if(seqno < head) {
                    if(log.isTraceEnabled()) {
                        StringBuffer sb=new StringBuffer("seqno ");
                        sb.append(seqno).append(" is smaller than ").append(head).append("); discarding message");
                        log.trace(sb.toString());
                    }
                    return;
                }

                // add at end (regular expected msg)
                if(seqno == tail) {
                    received_msgs.put(Long.valueOf(seqno), msg);
                    tail++;
                    if(highest_seen+2 == tail) {
                        highest_seen++;
                    }
                    else {
                       updateHighestSeen();
                    }

                    // highest_seen=seqno;
                }
                // gap detected
                // i. add placeholders, creating gaps
                // ii. add real msg
                // iii. tell retransmitter to retrieve missing msgs
                else if(seqno > tail) {
                    for(long i=tail; i < seqno; i++) {
                        received_msgs.put(Long.valueOf(i), null);
                        // XmitEntry xmit_entry=new XmitEntry();
                        //xmits.put(Long.valueOf(i), xmit_entry);
                        tail++;
                    }
                    received_msgs.put(Long.valueOf(seqno), msg);
                    tail=seqno + 1;
                    if(retransmitter != null) {
                        retransmitter.add(old_tail, seqno - 1);
                    }
                }
                else if(seqno < tail) { // finally received missing message
                    if(log.isTraceEnabled()) {
                        log.trace(new StringBuffer("added missing msg ").append(msg.getSrc()).append('#').append(seqno));
                    }
                    if(listener != null) {
                        try {listener.missingMessageReceived(seqno, msg);
                        } 
                        catch (VirtualMachineError err) { // GemStoneAddition
                          // If this ever returns, rethrow the error.  We're poisoned
                          // now, so don't let this thread continue.
                          throw err;
                        }
                        catch(Throwable t) {
                        }
                    }

                    Object val=received_msgs.get(Long.valueOf(seqno));
                    if(val == null) {
                        // only set message if not yet received (bela July 23 2003)
                        received_msgs.put(Long.valueOf(seqno), msg);

                        if(highest_seen +1 == seqno || seqno == head)
                            updateHighestSeen();

                        //XmitEntry xmit_entry=(XmitEntry)xmits.get(Long.valueOf(seqno));
                        //if(xmit_entry != null)
                        //  xmit_entry.received=System.currentTimeMillis();
                        //long xmit_diff=xmit_entry == null? -1 : xmit_entry.received - xmit_entry.created;
                        //NAKACK.addXmitResponse(msg.getSrc(), seqno);
                        if(retransmitter != null) retransmitter.remove(seqno);
                    }
                }
                updateLowestSeen();
            }
            finally {
                lock.writeLock().release();
            }
        }
        catch(InterruptedException e) {
            //log.error("failed acquiring write lock", e); GemStoneAddition - interrupts come at shutdown time, so why log this?
            Thread.currentThread().interrupt(); // GemStoneAddition
        }
    }


    /** Start from the current sequence number and set highest_seen until we find a gap (null value in the entry) */
    void updateHighestSeen() {
        SortedMap map=received_msgs.tailMap(Long.valueOf(highest_seen));
        Map.Entry entry;
        for(Iterator it=map.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            if(entry.getValue() != null)
                highest_seen=((Long)entry.getKey()).longValue();
            else
                break;
        }
    }

    public Message remove() {
        Message retval=null;
        Long    key;
        boolean bounded_buffer_enabled=max_xmit_buf_size > 0;

        try {
            lock.writeLock().acquire();
            try {
                while(received_msgs.size() > 0) {
                    key=(Long)received_msgs.firstKey();
                    retval=(Message)received_msgs.get(key);
                    if(retval != null) { // message exists and is ready for delivery
                        received_msgs.remove(key);       // move from received_msgs to ...
                        if(discard_delivered_msgs == false) {
                            delivered_msgs.put(key, retval); // delivered_msgs
                        }
                        head++;  // is removed from retransmitter somewhere else (when missing message is received)
                        return retval;
                    }
                    else { // message has not yet been received (gap in the message sequence stream)
                        if(bounded_buffer_enabled && received_msgs.size() > max_xmit_buf_size) {
                            received_msgs.remove(key);       // move from received_msgs to ...
                            head++;
                            retransmitter.remove(key.longValue());
                        }
                        else {
                            break;
                        }
                    }
                }
                return retval;
            }
            finally {
                lock.writeLock().release();
            }
        }
        catch(InterruptedException e) {
            //GemStoneAddition - this happens during shutdown
            //log.error("failed acquiring write lock", e);
          Thread.currentThread().interrupt(); // GemStoneAddition
           return null;
        }
    }



    /**
     * Delete all messages <= seqno (they are stable, that is, have been
     * received at all members). Stop when a number > seqno is encountered
     * (all messages are ordered on seqnos).
     */
    public void stable(long seqno) {
        try {
            lock.writeLock().acquire();
            try {
                // we need to remove all seqnos *including* seqno: because headMap() *excludes* seqno, we
                // simply increment it, so we have to correct behavior
                SortedMap m=delivered_msgs.headMap(Long.valueOf(seqno +1));
                if(m.size() > 0)
                    lowest_seen=Math.max(lowest_seen, ((Long)m.lastKey()).longValue());
                m.clear(); // removes entries from delivered_msgs
            }
            finally {
                lock.writeLock().release();
            }
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
            log.error(ExternalStrings.NakReceiverWindow_FAILED_ACQUIRING_WRITE_LOCK, e);
        }
    }


    /**
     * Reset the retransmitter and the nak window<br>
     */
    public void reset() {
        try {
            lock.writeLock().acquire();
            try {
                if(retransmitter != null)
                    retransmitter.reset();
                _reset();
            }
            finally {
                lock.writeLock().release();
            }
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
            log.error(ExternalStrings.NakReceiverWindow_FAILED_ACQUIRING_WRITE_LOCK, e);
        }
    }


    /**
     * Stop the retransmitter and reset the nak window<br>
     */
    public void destroy() {
        try {
            lock.writeLock().acquire();
            try {
                if(retransmitter != null)
                    retransmitter.stop();
                _reset();
            }
            finally {
                lock.writeLock().release();
            }
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
            log.error(ExternalStrings.NakReceiverWindow_FAILED_ACQUIRING_WRITE_LOCK, e);
        }
    }


    /**
     * @return the highest sequence number of a message consumed by the
     * application (by <code>remove()</code>)
     */
    public long getHighestDelivered() {
        try {
            lock.readLock().acquire();
            try {
                return (Math.max(head - 1, -1));
            }
            finally {
                lock.readLock().release();
            }
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
            log.error(ExternalStrings.NakReceiverWindow_FAILED_ACQUIRING_READ_LOCK, e);
            return -1;
        }
    }


    /**
     * @return the lowest sequence number of a message that has been
     * delivered or is a candidate for delivery (by the next call to
     * <code>remove()</code>)
     */
    public long getLowestSeen() {
        try {
            lock.readLock().acquire();
            try {
                return (lowest_seen);
            }
            finally {
                lock.readLock().release();
            }
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
            log.error(ExternalStrings.NakReceiverWindow_FAILED_ACQUIRING_READ_LOCK, e);
            return -1;
        }
    }


    /**
     * Returns the highest deliverable seqno; e.g., for 1,2,3,5,6 it would
     * be 3.
     *
     * @see NakReceiverWindow#getHighestReceived
     */
    public long getHighestSeen() {
        try {
            lock.readLock().acquire();
            try {
                return (highest_seen);
            }
            finally {
                lock.readLock().release();
            }
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
            log.error(ExternalStrings.NakReceiverWindow_FAILED_ACQUIRING_READ_LOCK, e);
            return -1;
        }
    }


    /**
     * Find all messages between 'low' and 'high' (including 'low' and
     * 'high') that have a null msg.
     * Return them as a list of longs
     *
     * @return List<Long>. A list of seqnos, sorted in ascending order.
     * E.g. [1, 4, 7, 8]
     */
    public List getMissingMessages(long low, long high) {
        List retval=new List();
        // long my_high;

        if(low > high) {
            if(log.isErrorEnabled()) log.error("invalid range: low (" + low +
                    ") is higher than high (" + high + ')');
            return null;
        }

        try {
            lock.readLock().acquire();
            try {

                // my_high=Math.max(head - 1, 0);
                // check only received messages, because delivered messages *must* have a non-null msg
                SortedMap m=received_msgs.subMap(Long.valueOf(low), Long.valueOf(high+1));
                for(Iterator it=m.keySet().iterator(); it.hasNext();) {
                    retval.add(it.next());
                }

//            if(received_msgs.size() > 0) {
//                entry=(Entry)received_msgs.peek();
//                if(entry != null) my_high=entry.seqno;
//            }
//            for(long i=my_high + 1; i <= high; i++)
//                retval.add(Long.valueOf(i));

                return retval;
            }
            finally {
                lock.readLock().release();
            }
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
            log.error(ExternalStrings.NakReceiverWindow_FAILED_ACQUIRING_READ_LOCK, e);
            return null;
        }
    }


    /**
     * Returns the highest sequence number received so far (which may be
     * higher than the highest seqno <em>delivered</em> so far; e.g., for
     * 1,2,3,5,6 it would be 6.
     *
     * @see NakReceiverWindow#getHighestSeen
     */
    public long getHighestReceived() {
        try {
            lock.readLock().acquire();
            try {
                return Math.max(tail - 1, -1);
            }
            finally {
                lock.readLock().release();
            }
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
          // GemStoneAddition - do not log errors since they may raise alerts
          // and the only time this method has ever been interrupted has been
          // during shutdown
          // log.error(JGroupsStrings.NakReceiverWindow_FAILED_ACQUIRING_READ_LOCK, e);
            return -1;
        }
    }


    /**
     * Return messages that are higher than <code>seqno</code> (excluding
     * <code>seqno</code>). Check both received <em>and</em> delivered
     * messages.
     * @return List<Message>. All messages that have a seqno greater than <code>seqno</code>
     */
    public List getMessagesHigherThan(long seqno) {
        List retval=new List();

        try {
            lock.readLock().acquire();
            try {
                // check received messages
                SortedMap m=received_msgs.tailMap(Long.valueOf(seqno+1));
                for(Iterator it=m.values().iterator(); it.hasNext();) {
                    retval.add((it.next()));
                }

                // we retrieve all msgs whose seqno is strictly greater than seqno (tailMap() *includes* seqno,
                // but we need to exclude seqno, that's why we increment it
                m=delivered_msgs.tailMap(Long.valueOf(seqno +1));
                for(Iterator it=m.values().iterator(); it.hasNext();) {
                    retval.add(((Message)it.next()).copy());
                }
                return (retval);

            }
            finally {
                lock.readLock().release();
            }
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
            log.error(ExternalStrings.NakReceiverWindow_FAILED_ACQUIRING_READ_LOCK, e);
            return null;
        }
    }


    /**
     * Return all messages m for which the following holds:
     * m > lower && m <= upper (excluding lower, including upper). Check both
     * <code>received_msgs</code> and <code>delivered_msgs</code>.
     */
    public List getMessagesInRange(long lower, long upper) {
        List retval=new List();

        try {
            lock.readLock().acquire();
            try {
                // check received messages
                SortedMap m=received_msgs.subMap(Long.valueOf(lower +1), Long.valueOf(upper +1));
                for(Iterator it=m.values().iterator(); it.hasNext();) {
                    retval.add(it.next());
                }

                m=delivered_msgs.subMap(Long.valueOf(lower +1), Long.valueOf(upper +1));
                for(Iterator it=m.values().iterator(); it.hasNext();) {
                    retval.add(((Message)it.next()).copy());
                }
                return retval;

            }
            finally {
                lock.readLock().release();
            }
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
            log.error(ExternalStrings.NakReceiverWindow_FAILED_ACQUIRING_READ_LOCK, e);
            return null;
        }
    }


    /**
     * Return a list of all messages for which there is a seqno in
     * <code>missing_msgs</code>. The seqnos of the argument list are
     * supposed to be in ascending order
     * @param missing_msgs A List<Long> of seqnos
     * @return List<Message>
     */
    public List getMessagesInList(List missing_msgs) {
        List ret=new List();

        if(missing_msgs == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.NakReceiverWindow_ARGUMENT_LIST_IS_NULL);
            return ret;
        }

        try {
            lock.readLock().acquire();
            try {
                Long seqno;
                Message msg;
                for(Enumeration en=missing_msgs.elements(); en.hasMoreElements();) {
                    seqno=(Long)en.nextElement();
                    msg=(Message)delivered_msgs.get(seqno);
                    if(msg != null)
                        ret.add(msg.copy());
                    msg=(Message)received_msgs.get(seqno);
                    if(msg != null)
                        ret.add(msg.copy());
                }
                return ret;
            }
            finally {
                lock.readLock().release();
            }
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
            log.error(ExternalStrings.NakReceiverWindow_FAILED_ACQUIRING_READ_LOCK, e);
            return null;
        }
    }

    /**
     * Returns the message from received_msgs or delivered_msgs.
     * @param sequence_num
     * @return Message from received_msgs or delivered_msgs.
     */
    public Message get(long sequence_num) {
        Message msg;
        Long seqno=Long.valueOf(sequence_num);
        try {
            lock.readLock().acquire();
            try {
                msg=(Message)delivered_msgs.get(seqno);
                if(msg != null)
                    return msg;
                msg=(Message)received_msgs.get(seqno);
                if(msg != null)
                    return msg;
            }
            finally {
                lock.readLock().release();
            }
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
            log.error(ExternalStrings.NakReceiverWindow_FAILED_ACQUIRING_READ_LOCK, e);
        }
        return null;
    }


    public int size() {
        boolean acquired=false;
        try {
            lock.readLock().acquire();
            acquired=true;
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
        }
        try {
            return received_msgs.size();
        }
        finally {
            if(acquired)
                lock.readLock().release();
        }
    }


    /**
     * For statistics use only since it doesn't lock the list
     * @return size of received_msgs
     */
    public int unsafeGetSize() { return received_msgs.size(); }
    
    @Override // GemStoneAddition
    public String toString() {
        StringBuffer sb=new StringBuffer();
        try {
            lock.readLock().acquire();
            try {
                sb.append("received_msgs: " + printReceivedMessages());
                sb.append(", delivered_msgs: " + printDeliveredMessages());
            }
            finally {
                lock.readLock().release();
            }
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
            log.error(ExternalStrings.NakReceiverWindow_FAILED_ACQUIRING_READ_LOCK, e);
            return "";
        }

        return sb.toString();
    }


    /**
     * Prints delivered_msgs. Requires read lock present.
     * @return the representative string
     */
    String printDeliveredMessages() {
        StringBuffer sb=new StringBuffer();
        Long min=null, max=null;

        if(delivered_msgs.size() > 0) {
            try {min=(Long)delivered_msgs.firstKey();} catch(NoSuchElementException ex) {}
            try {max=(Long)delivered_msgs.lastKey();}  catch(NoSuchElementException ex) {}
        }
        sb.append('[').append(min).append(" - ").append(max).append(']');
        if(min != null && max != null)
            sb.append(" (size=" + (max.longValue() - min.longValue()) + ")");
        return sb.toString();
    }


    /**
     * Prints received_msgs. Requires read lock to be present
     * @return the print string
     */
    String printReceivedMessages() {
        StringBuffer sb=new StringBuffer();
        sb.append('[');
        if(received_msgs.size() > 0) {
            Long first=null, last=null;
            try {first=(Long)received_msgs.firstKey();} catch(NoSuchElementException ex) {}
            try {last=(Long)received_msgs.lastKey();}   catch(NoSuchElementException ex) {}
            sb.append(first).append(" - ").append(last);
            int non_received=0;
            Map.Entry entry;

            for(Iterator it=received_msgs.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                if(entry.getValue() == null)
                    non_received++;
            }
            sb.append(" (size=").append(received_msgs.size()).append(", missing=").append(non_received).append(')');
        }
        sb.append(']');
        return sb.toString();
    }

    /* ------------------------------- Private Methods -------------------------------------- */


    /**
     * Sets the value of lowest_seen to the lowest seqno of the delivered messages (if available), otherwise
     * to the lowest seqno of received messages.
     */
    private void updateLowestSeen() {
        Long  lowest_seqno=null;

        // If both delivered and received messages are empty, let the highest
        // seen seqno be the one *before* the one which is expected to be
        // received next by the NakReceiverWindow (head-1)

        // incorrect: if received and delivered msgs are empty, don't do anything: we may have initial values,
        // but both lists are cleaned after some time of inactivity
        // (bela April 19 2004)
        /*
        if((delivered_msgs.size() == 0) && (msgs.size() == 0)) {
            lowest_seen=0;
            return;
        }
        */

        // The lowest seqno is the first seqno of the delivered messages
        if(delivered_msgs.size() > 0) {
            try {
                lowest_seqno=(Long)delivered_msgs.firstKey();
                if(lowest_seqno != null)
                    lowest_seen=lowest_seqno.longValue();
            }
            catch(NoSuchElementException ex) {
            }
        }
        // If no elements in delivered messages (e.g. due to message garbage collection), use the received messages
        else {
            if(received_msgs.size() > 0) {
                try {
                    lowest_seqno=(Long)received_msgs.firstKey();
                    if(received_msgs.get(lowest_seqno) != null) { // only set lowest_seen if we *have* a msg
                        lowest_seen=lowest_seqno.longValue();
                    }
                }
                catch(NoSuchElementException ex) {}
            }
        }
    }


    /**
     * Find the highest seqno that is deliverable or was actually delivered.
     * Returns seqno-1 if there are no messages in the queues (the first
     * message to be expected is always seqno).
     */
//    private void updateHighestSeen() {
//        long      ret=0;
//        Map.Entry entry=null;
//
//        // If both delivered and received messages are empty, let the highest
//        // seen seqno be the one *before* the one which is expected to be
//        // received next by the NakReceiverWindow (head-1)
//
//        // changed by bela (April 19 2004): we don't change the value if received and delivered msgs are empty
//        /*if((delivered_msgs.size() == 0) && (msgs.size() == 0)) {
//            highest_seen=0;
//            return;
//        }*/
//
//
//        // The highest seqno is the last of the delivered messages, to start with,
//        // or again the one before the first seqno expected (if no delivered
//        // msgs). Then iterate through the received messages, and find the highest seqno *before* a gap
//        Long highest_seqno=null;
//        if(delivered_msgs.size() > 0) {
//            try {
//                highest_seqno=(Long)delivered_msgs.lastKey();
//                ret=highest_seqno.longValue();
//            }
//            catch(NoSuchElementException ex) {
//            }
//        }
//        else {
//            ret=Math.max(head - 1, 0);
//        }
//
//        // Now check the received msgs head to tail. if there is an entry
//        // with a non-null msg, increment ret until we find an entry with
//        // a null msg
//        for(Iterator it=received_msgs.entrySet().iterator(); it.hasNext();) {
//            entry=(Map.Entry)it.next();
//            if(entry.getValue() != null)
//                ret=((Long)entry.getKey()).longValue();
//            else
//                break;
//        }
//        highest_seen=Math.max(ret, 0);
//    }


    /**
     * Reset the Nak window. Should be called from within a writeLock() context.
     * <p>
     * i. Delete all received entries<br>
     * ii. Delete alll delivered entries<br>
     * iii. Reset all indices (head, tail, etc.)<br>
     */
    private void _reset() {
        received_msgs.clear();
        delivered_msgs.clear();
        head=0;
        tail=0;
        lowest_seen=0;
        highest_seen=0;
    }
    /* --------------------------- End of Private Methods ----------------------------------- */


}
