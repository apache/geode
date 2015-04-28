/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: AckMcastSenderWindow.java,v 1.9 2005/07/17 11:34:20 chrislott Exp $

package com.gemstone.org.jgroups.stack;


import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.util.TimeScheduler;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;




/**
 * Keeps track of ACKs from receivers for each message. When a new message is
 * sent, it is tagged with a sequence number and the receiver set (set of
 * members to which the message is sent) and added to a hashtable
 * (key = sequence number, val = message + receiver set). Each incoming ACK
 * is noted and when all ACKs for a specific sequence number haven been
 * received, the corresponding entry is removed from the hashtable. A
 * retransmission thread periodically re-sends the message point-to-point to
 * all receivers from which no ACKs have been received yet. A view change or
 * suspect message causes the corresponding non-existing receivers to be
 * removed from the hashtable.
 * <p>
 * This class may need flow control in order to avoid needless
 * retransmissions because of timeouts.
 *
 * @author Bela Ban June 9 1999
 * @author John Georgiadis May 8 2001
 * @version $Revision: 1.9 $
 */
public class AckMcastSenderWindow  {
    /**
     * Called by retransmitter thread whenever a message needs to be re-sent
     * to a destination. <code>dest</code> has to be set in the
     * <code>dst</code> field of <code>msg</code>, as the latter was sent
     * multicast, but now we are sending a unicast message. Message has to be
     * copied before sending it (as headers will be appended and therefore
     * the message changed!).
     */
    public interface RetransmitCommand {
	/**
	 * Retranmit the given msg
	 *
	 * @param seqno the sequence number associated with the message
	 * @param msg the msg to retransmit (it should be a copy!)
	 * @param dest the msg destination
	 */
	void retransmit(long seqno, Message msg, Address dest);
    }


    /**
     * The retransmit task executed by the scheduler in regular intervals
     */
    private static abstract class Task implements TimeScheduler.Task {
	private final Interval intervals;
	private boolean  cancelled;

	protected Task(long[] intervals) {
	    this.intervals = new Interval(intervals);
	    this.cancelled = false;
	}
	public long nextInterval() { return(intervals.next()); }
	public void cancel() { cancelled = true; }
	public boolean cancelled() { return(cancelled); }
    }


    /**
     * The entry associated with a pending msg
     */
    private class Entry extends Task  {
	/** The msg sequence number */
	public final long seqno;
	/** The msg to retransmit */
	public Message msg = null;
	/** destination addr -> boolean (true = received, false = not) */
	public final Hashtable senders = new Hashtable();
	/** How many destinations have received the msg */
	public int num_received = 0;

	public Entry(long seqno, Message msg, Vector dests, long[] intervals) {
	    super(intervals);
	    this.seqno = seqno;
	    this.msg   = msg;
	    for (int i = 0; i < dests.size(); i++)
		senders.put(dests.elementAt(i), Boolean.FALSE);
	}

	boolean allReceived() {
	    return(num_received >= senders.size());
	}

	/** Retransmit this entry */
	public void run() { _retransmit(this); }

	    @Override // GemStoneAddition
	public String toString() {
	    StringBuffer buff = new StringBuffer();
	    buff.append("num_received = " + num_received +
			", received msgs = " + senders);
	    return(buff.toString());
	}
    }

    

    private static final long SEC = 1000;
    /** Default retransmit intervals (ms) - exponential approx. */
    private static final long[] RETRANSMIT_TIMEOUTS = {
	2*SEC,
	3*SEC,
	5*SEC,
	8*SEC};
    /** Default retransmit thread suspend timeout (ms) */
    private static final long SUSPEND_TIMEOUT = 2000;

    protected static final GemFireTracer log=GemFireTracer.getLog(AckMcastSenderWindow.class);


    // Msg tables related
    /** Table of pending msgs: seqno -> Entry */
    private final Hashtable  msgs = new Hashtable();

    /** List of recently suspected members. Used to cease retransmission to suspected members */
    private final LinkedList suspects=new LinkedList();

    /** Max number in suspects list */
    private final int max_suspects=20;

    /**
     * List of acknowledged msgs since the last call to
     * <code>getStableMessages()</code>
     */
    private final Vector stable_msgs = new Vector();
    /** Whether a call to <code>waitUntilAcksReceived()</code> is still active */
    private boolean waiting = false;

    // Retransmission thread related
    /** Whether retransmitter is externally provided or owned by this object */
    private boolean retransmitter_owned;
    /** The retransmission scheduler */
    private TimeScheduler retransmitter = null;
    /** Retransmission intervals */
    private long[] retransmit_intervals;
    /** The callback object for retransmission */
    private RetransmitCommand cmd = null;


    /**
     * Convert exception stack trace to string
     */
    private static String _toString(Throwable ex) {
	StringWriter sw = new StringWriter();
	PrintWriter  pw = new PrintWriter(sw);
	ex.printStackTrace(pw);
	return(sw.toString());
    }


    /**
     * @param entry the record associated with the msg to retransmit. It
     * contains the list of receivers that haven't yet ack reception
     */
    protected/*GemStoneAddition*/ void _retransmit(Entry entry) {
	Address sender;
	boolean received;

	synchronized(entry) {
	    for(Enumeration e = entry.senders.keys(); e.hasMoreElements();) {
		sender   = (Address)e.nextElement();
		received = ((Boolean)entry.senders.get(sender)).booleanValue();
		if (!received) {
		    if(suspects.contains(sender)) {

			    if(log.isWarnEnabled()) log.warn("removing " + sender +
				       " from retransmit list as it is in the suspect list");
			remove(sender);
			continue;
		    }

			if(log.isInfoEnabled()) log.info("--> retransmitting msg #" +
				   entry.seqno + " to " + sender);
		    cmd.retransmit(entry.seqno, entry.msg.copy(), sender);
		}
	    }
	}
    }


    /**
     * Setup this object's state
     *
     * @param cmd the callback object for retranmissions
     * @param retransmit_intervals the interval between two consecutive
     * retransmission attempts
     * @param sched the external scheduler to use to schedule retransmissions
     * @param sched_owned if true, the scheduler is owned by this object and
     * can be started/stopped/destroyed. If false, the scheduler is shared
     * among multiple objects and start()/stop() should not be called from
     * within this object
     *
     * @throws IllegalArgumentException if <code>cmd</code> is null
     */
    private void init(RetransmitCommand cmd, long[] retransmit_intervals,
		      TimeScheduler sched, boolean sched_owned) {
	if (cmd == null) {
	    if(log.isErrorEnabled()) log.error(ExternalStrings.AckMcastSenderWindow_COMMAND_IS_NULL_CANNOT_RETRANSMIT_MESSAGES_);
	    throw new IllegalArgumentException("cmd");
	}

	retransmitter_owned = sched_owned;
	retransmitter = sched;
	this.retransmit_intervals = retransmit_intervals;
	this.cmd = cmd;

	start();
    }


    /**
     * Create and <b>start</b> the retransmitter
     *
     * @param cmd the callback object for retranmissions
     * @param retransmit_intervals the interval between two consecutive
     * retransmission attempts
     * @param sched the external scheduler to use to schedule retransmissions
     *
     * @throws IllegalArgumentException if <code>cmd</code> is null
     */
    public AckMcastSenderWindow(RetransmitCommand cmd,
				long[] retransmit_intervals, TimeScheduler sched) {
	init(cmd, retransmit_intervals, sched, false);
    }


    /**
     * Create and <b>start</b> the retransmitter
     *
     * @param cmd the callback object for retranmissions
     * @param sched the external scheduler to use to schedule retransmissions
     *
     * @throws IllegalArgumentException if <code>cmd</code> is null
     */
    public AckMcastSenderWindow(RetransmitCommand cmd, TimeScheduler sched) {
	init(cmd, RETRANSMIT_TIMEOUTS, sched, false);
    }



    /**
     * Create and <b>start</b> the retransmitter
     *
     * @param cmd the callback object for retranmissions
     * @param retransmit_intervals the interval between two consecutive
     * retransmission attempts
     *
     * @throws IllegalArgumentException if <code>cmd</code> is null
     */
    public AckMcastSenderWindow(RetransmitCommand cmd, long[] retransmit_intervals) {
	init(cmd, retransmit_intervals, new TimeScheduler(SUSPEND_TIMEOUT), true);
    }

    /**
     * Create and <b>start</b> the retransmitter
     *
     * @param cmd the callback object for retranmissions
     *
     * @throws IllegalArgumentException if <code>cmd</code> is null
     */
    public AckMcastSenderWindow(RetransmitCommand cmd) {
	this(cmd, RETRANSMIT_TIMEOUTS);
    }


    /**
     * Adds a new message to the hash table.
     *
     * @param seqno The sequence number associated with the message
     * @param msg The message (should be a copy!)
     * @param receivers The set of addresses to which the message was sent
     * and from which consequently an ACK is expected
     */
    public void add(long seqno, Message msg, Vector receivers) {
	Entry e;

	if (waiting) return;
	if (receivers.size() == 0) return;

	synchronized(msgs) {
	    if (msgs.get(Long.valueOf(seqno)) != null) return;
	    e = new Entry(seqno, msg, receivers, retransmit_intervals);
	    msgs.put(Long.valueOf(seqno), e);
	    retransmitter.add(e);
	}
    }


    /**
     * An ACK has been received from <code>sender</code>. Tag the sender in
     * the hash table as 'received'. If all ACKs have been received, remove
     * the entry all together.
     *
     * @param seqno  The sequence number of the message for which an ACK has
     * been received.
     * @param sender The sender which sent the ACK
     */
    public void ack(long seqno, Address sender) {
	Entry   entry;
	Boolean received;

	synchronized(msgs) {
	    entry = (Entry)msgs.get(Long.valueOf(seqno));
	    if (entry == null) return;
		    
	    synchronized(entry) {
		received = (Boolean)entry.senders.get(sender);
		if (received == null || received.booleanValue()) return;
			
		// If not yet received
		entry.senders.put(sender, Boolean.TRUE);
		entry.num_received++;
		if (!entry.allReceived()) return;
	    }
		    
	    synchronized(stable_msgs) {
		entry.cancel();
		msgs.remove(Long.valueOf(seqno));
		stable_msgs.add(Long.valueOf(seqno));
	    }
	    // wake up waitUntilAllAcksReceived() method
	    msgs.notifyAll();
	}
    }
    

    /**
     * Remove <code>obj</code> from all receiver sets and wake up
     * retransmission thread.
     *
     * @param obj the sender to remove
     */
    public void remove(Address obj) {
	Long  key;
	Entry entry;

	synchronized(msgs) {
	    for (Enumeration e = msgs.keys(); e.hasMoreElements();) {
		key   = (Long)e.nextElement();
		entry = (Entry)msgs.get(key);
		synchronized(entry) {
		    //if (((Boolean)entry.senders.remove(obj)).booleanValue()) entry.num_received--;
		    //if (!entry.allReceived()) continue;
		    Boolean received = (Boolean)entry.senders.remove(obj);
		    if(received == null) continue; // suspected member not in entry.senders ?
		    if (received.booleanValue()) entry.num_received--;
		    if (!entry.allReceived()) continue;
		}
		synchronized(stable_msgs) {
		    entry.cancel();
		    msgs.remove(key);
		    stable_msgs.add(key);
		}
		// wake up waitUntilAllAcksReceived() method
		msgs.notifyAll();
	    }
	}
    }


    /**
     * Process with address <code>suspected</code> is suspected: remove it
     * from all receiver sets. This means that no ACKs are expected from this
     * process anymore.
     *
     * @param suspected The suspected process
     */
    public void suspect(Address suspected) {

	    if(log.isInfoEnabled()) log.info(ExternalStrings.AckMcastSenderWindow_SUSPECT_IS__0, suspected);
	remove(suspected);
	suspects.add(suspected);
	if(suspects.size() >= max_suspects)
	    suspects.removeFirst();
    }


    /**
     * @return a copy of stable messages, or null (if non available). Removes
     * all stable messages afterwards
     */
    public Vector getStableMessages() {
	Vector retval;

	synchronized(stable_msgs) {
	    retval = (stable_msgs.size() > 0)? (Vector)stable_msgs.clone():null;
	    if (stable_msgs.size() > 0) stable_msgs.clear();
	}
		
	return(retval);
    }


    public void clearStableMessages() {
	synchronized(stable_msgs) {
	    stable_msgs.clear();
	}
    }


    /**
     * @return the number of currently pending msgs
     */
    public long size() {
	synchronized(msgs) {
	    return(msgs.size());
	}
    }


    /** Returns the number of members for a given entry for which acks have to be received */
    public long getNumberOfResponsesExpected(long seqno) {
	Entry entry=(Entry)msgs.get(Long.valueOf(seqno));
	if(entry != null)
	    return entry.senders.size();
	else
	    return -1;
    }

    /** Returns the number of members for a given entry for which acks have been received */
    public long getNumberOfResponsesReceived(long seqno) {
	Entry entry=(Entry)msgs.get(Long.valueOf(seqno));
	if(entry != null)
	    return entry.num_received;
	else
	    return -1;
    }

    /** Prints all members plus whether an ack has been received from those members for a given seqno */
    public String printDetails(long seqno) {
	Entry entry=(Entry)msgs.get(Long.valueOf(seqno));
	if(entry != null)
	    return entry.toString();
	else
	    return null;
    }


    /**
     * Waits until all outstanding messages have been ACKed by all receivers.
     * Takes into account suspicions and view changes. Returns when there are
     * no entries left in the hashtable. <b>While waiting, no entries can be
     * added to the hashtable (they will be discarded).</b>
     *
     * @param timeout Miliseconds to wait. 0 means wait indefinitely.
     */
    public void waitUntilAllAcksReceived(long timeout) {
	long    time_to_wait, start_time, current_time;
	Address suspect;

	// remove all suspected members from retransmission
	for(Iterator it=suspects.iterator(); it.hasNext();) {
	    suspect=(Address)it.next();
	    remove(suspect);
	}
	
	time_to_wait = timeout;
	waiting     = true;
	if (timeout <= 0) {
	    synchronized(msgs) {
		while(msgs.size() > 0)
		    try { msgs.wait(); } catch(InterruptedException ex) {
		      Thread.currentThread().interrupt(); // GemStoneAddition
                      return; // GemStoneAddition
                    }
	    }
	} else {
	    start_time = System.currentTimeMillis();
	    synchronized(msgs) {
		while(msgs.size() > 0) {
		    current_time = System.currentTimeMillis();
		    time_to_wait = timeout - (current_time - start_time);
		    if (time_to_wait <= 0) break;
		    
		    try {
			msgs.wait(time_to_wait);
		    } catch(InterruptedException ex) {
			if(log.isWarnEnabled()) log.warn(ex.toString());
                        Thread.currentThread().interrupt(); // GemStoneAddition
                        return; // GemStoneAddition
		    }
		}
	    }
	}	
	waiting = false;
    }




    /**
     * Start the retransmitter. This has no effect, if the retransmitter
     * was externally provided
     */
    public void start() {
	if (retransmitter_owned)
	    retransmitter.start();
    }


    /**
     * Stop the rentransmition and clear all pending msgs.
     * <p>
     * If this retransmitter has been provided an externally managed
     * scheduler, then just clear all msgs and the associated tasks, else
     * stop the scheduler. In this case the method blocks until the
     * scheduler's thread is dead. Only the owner of the scheduler should
     * stop it.
     */
    public void stop() {
	Entry entry;

	// i. If retransmitter is owned, stop it else cancel all tasks
	// ii. Clear all pending msgs and notify anyone waiting
	synchronized(msgs) {
	    if (retransmitter_owned) {
		try {
		    retransmitter.stop();
		} catch(InterruptedException ex) {
                  // GemStoneAddition (explanation)
                  // We're trying to stop.  No loops involved.
                  // Don't bother setting interrupt bit, keep going.
                  
		    if(log.isErrorEnabled()) log.error(_toString(ex));
		}
	    } else {
		for (Enumeration e = msgs.elements(); e.hasMoreElements();) {
		    entry = (Entry)e.nextElement();
		    entry.cancel();
		}
	    }
	    msgs.clear();
	    // wake up waitUntilAllAcksReceived() method
	    msgs.notifyAll();
	}
    }


    /**
     * Remove all pending msgs from the hashtable. Cancel all associated
     * tasks in the retransmission scheduler
     */
    public void reset() {
	Entry entry;

	if (waiting) return;

	synchronized(msgs) {
	    for (Enumeration e = msgs.elements(); e.hasMoreElements();) {
		entry = (Entry)e.nextElement();
		entry.cancel();
	    }
	    msgs.clear();
	    msgs.notifyAll();
	}
    }


    @Override // GemStoneAddition
    public String toString() {
	StringBuffer ret;
	Entry        entry;
	Long         key;

	ret = new StringBuffer();
	synchronized(msgs) {
	    ret.append("msgs: (" + msgs.size() + ')');
	    for (Enumeration e = msgs.keys(); e.hasMoreElements();) {
		key   = (Long)e.nextElement();
		entry = (Entry)msgs.get(key);
		ret.append("key = " + key + ", value = " + entry + '\n');
	    }
	    synchronized(stable_msgs) {
		ret.append("\nstable_msgs: " + stable_msgs);
	    }
	}
		
	return(ret.toString());
    }
}
