/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: FLUSH.java,v 1.10 2005/08/11 12:43:47 belaban Exp $



package com.gemstone.org.jgroups.protocols;



import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.blocks.GroupRequest;
import com.gemstone.org.jgroups.blocks.MethodCall;
import com.gemstone.org.jgroups.stack.RpcProtocol;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.List;
import com.gemstone.org.jgroups.util.Rsp;
import com.gemstone.org.jgroups.util.RspList;
import com.gemstone.org.jgroups.util.Util;

import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;




/**
   The task of the FLUSH protocol is to flush all pending messages out of the system. This is
   done before a view change by stopping all senders and then agreeing on what messages
   should be delivered in the current view (before switching to the new view). A coordinator
   broadcasts a FLUSH message. The message contains an array of the highest sequence number for each member
   as seen by the coordinator so far. Each member responds with its highest sequence numbers seen so far (for
   each member): if its sequence number for a member P is higher than the one sent by the coordinator, it
   will append the messages apparently not received by the coordinator to its reply. The coordinator (when
   all replies have been received), computes for each member the lowest and highest sequence number and
   re-broadcasts messages accordingly (using ACKs rather then NAKs to ensure reliable delivery).<p> Example:
   <pre>

   FLUSH  ---> (p=10, q=22, r=7)

   <-- (p=10, q=20, r=7)    (did not receive 2 messages from q)
   <-- (p=12, q=23, r=7)    (attached are messages p11, p12, and q23)
   <-- (p=10, q=22, r=8)    (attached is message r8)
   ---------------------
   min:   11    21    8
   max:   12    23    8
   </pre>

   The coordinator now computes the range for each member and re-broadcasts messages
   p11, p12, q21, q22, q23 and r8.
   This is essentially the exclusive min and inclusive max of all replies. Note that messages p11, p12 and q23
   were not received by the coordinator itself before. They were only returned as result of the FLUSH replies
   and the coordinator now re-broadcasts them.

*/
public class FLUSH extends RpcProtocol  {
    final Vector   mbrs=new Vector();
    boolean  is_server=false;
    final Object   block_mutex=new Object();
    long     block_timeout=5000;
    Address  local_addr=null;
    boolean  blocked=false;  // BLOCK: true, VIEW_CHANGE: false
    final Object   digest_mutex=new Object();
    long     digest_timeout=2000;   // time to wait for retrieval of unstable msgs

    final Object   highest_delivered_mutex=new Object();
    long[]   highest_delivered_msgs;

    Digest   digest=null;

    final Object   get_msgs_mutex=new Object();
    static/*GemStoneAddition*/ final long     get_msgs_timeout=4000;
    List     get_msgs=null;



    @Override // GemStoneAddition
    public String  getName() {return "FLUSH";}


    @Override // GemStoneAddition
    public Vector providedUpServices() {
	Vector retval=new Vector();
	retval.addElement(Integer.valueOf(Event.FLUSH));
	return retval;
    }

    @Override // GemStoneAddition
    public Vector requiredDownServices() {
	Vector retval=new Vector();
	retval.addElement(Integer.valueOf(Event.GET_MSGS_RECEIVED));  // NAKACK layer
	retval.addElement(Integer.valueOf(Event.GET_MSG_DIGEST));     // NAKACK layer
	retval.addElement(Integer.valueOf(Event.GET_MSGS));           // NAKACK layer
	return retval;
    }


    @Override // GemStoneAddition
    public void start() throws Exception {
        super.start();
        if(_corr != null) {
            _corr.setDeadlockDetection(true);
        }
        else
            throw new Exception("FLUSH.start(): cannot set deadlock detection in corr, as it is null !");
    }


    /**
       Triggered by reception of FLUSH event from GMS layer (must be coordinator). Calls
       <code>HandleFlush</code> in all members and returns FLUSH_OK event.
       @param dests A list of members to which the FLUSH is to be sent
       @return FlushRsp Contains result (true or false), list of unstable messages and list of members
	       failed during the FLUSH.
     */
    private FlushRsp flush(Vector dests) {
	RspList     rsp_list;
	FlushRsp    retval=new FlushRsp();
	Digest      digest;
	long[]      min, max;
	long[]      lower[];
	List        unstable_msgs=new List();
	boolean     get_lower_msgs=false;

	highest_delivered_msgs=new long[members.size()];
	min=new long[members.size()];
	max=new long[members.size()];


	/* Determine the highest seqno (for each member) that was delivered to the application
	   (i.e., consumed by the application). Stores result in array 'highest_delivered_msgs' */
	getHighestDeliveredSeqnos();

	for(int i=0; i < highest_delivered_msgs.length; i++)
	    min[i]=max[i]=highest_delivered_msgs[i];


	/* Call the handleFlush() method of all existing members. The highest seqnos seen by the coord
	   is the argument */
	 if(log.isInfoEnabled()) log.info(ExternalStrings.FLUSH_CALLING_HANDLEFLUSH_0, dests);
	passDown(new Event(Event.SWITCH_OUT_OF_BAND)); // we need out-of-band control for FLUSH ...
	MethodCall call = new MethodCall("handleFlush", new Object[] {dests, highest_delivered_msgs.clone()}, 
		new String[] {Vector.class.getName(), long[].class.getName()});
	rsp_list=callRemoteMethods(dests, call, GroupRequest.GET_ALL, 0);
	 if(log.isInfoEnabled()) log.info(ExternalStrings.FLUSH_FLUSH_DONE);


	/* Process all the responses (Digest): compute a range of messages (min and max seqno) for each
	   member that has to be re-broadcast; FlushRsp contains those messages. They will be re-braodcast
	   by the cordinator (in the GMS protocol). */
	for(int i=0; i < rsp_list.size(); i++) {
	    Rsp rsp=(Rsp)rsp_list.elementAt(i);
	    if(rsp.wasReceived()) {
		digest=(Digest)rsp.getValue();
		if(digest != null) {
		    for(int j=0; j < digest.highest_seqnos.length && j < min.length; j++) {
			min[j]=Math.min(min[j], digest.highest_seqnos[j]);
			max[j]=Math.max(max[j], digest.highest_seqnos[j]);
		    }
		    if(digest.msgs.size() > 0) {
			for(Enumeration e=digest.msgs.elements(); e.hasMoreElements();)
			    unstable_msgs.add(e.nextElement());
		    }
		}
	    }
	} // end for-loop



	/* If any of the highest msgs of the flush replies were lower than the ones sent by this
	   coordinator, we have to re-broadcast them. (This won't occur often)
	   Compute the range between min and highest_delivered_msgs */
	lower=new long[min.length][]; // stores (for each mbr) the range of seqnos (e.g. 20 24): send msgs
				      // 21, 22 and 23 and 24 (excluding lower and including upper range)

	for(int i=0; i < min.length; i++) {
	    if(min[i] < highest_delivered_msgs[i]) {    // will almost never be the case
		lower[i]=new long[2];
		lower[i][0]=min[i];                     // lower boundary (excluding)
		lower[i][1]=highest_delivered_msgs[i];  // upper boundary (including)
		get_lower_msgs=true;
	    }
	}
	if(get_lower_msgs) {
	    get_msgs=null;
	    synchronized(get_msgs_mutex) {
		passDown(new Event(Event.GET_MSGS, lower));
		try {
		    get_msgs_mutex.wait(get_msgs_timeout);
		}
		catch(InterruptedException e) { // GemStoneAddition
                  Thread.currentThread().interrupt();
                  // There's no looping or anything in the rest of this
                  // method, so we just propagate the bit and finish up...
                }
	    }
	    if(get_msgs != null) {
		for(Enumeration e=get_msgs.elements(); e.hasMoreElements();)
		    unstable_msgs.add(e.nextElement());
	    }
	}
	retval.unstable_msgs=unstable_msgs.getContents();
	if(rsp_list.numSuspectedMembers() > 0) {
	    retval.result=false;
	    retval.failed_mbrs=rsp_list.getSuspectedMembers();
	}

	return retval;
    }





    /**
       Called by coordinator running the FLUSH protocol. Argument is an array of the highest seqnos as seen
       by the coordinator (for each member). <code>handleFlush()</code> checks for each member its
       own highest seqno seen for that member. If it is higher than the one seen by the coordinator,
       all higher messages are attached to the return value (a message digest).
       @param flush_dests  The members to which this message is sent. Processes not in this list just
			   ignore the handleFlush() message.
       @param highest_seqnos The highest sequence numbers (order corresponding to membership) as seen
			     by coordinator.
       @return Digest An array of the highest seqnos for each member, as seen by this member. If this
		      member's seqno for a member P is higher than the one in <code>highest_seqnos</code>,
		      the missing messages are added to the message digest as well. This allows the
		      coordinator to re-broadcast missing messages.
     */
    public synchronized Digest handleFlush(Vector flush_dests, long[] highest_seqnos) {
	digest=null;

	 if(log.isInfoEnabled()) log.info("flush_dests=" + flush_dests +
				   " , highest_seqnos=" + Util.array2String(highest_seqnos));

	if(!is_server) // don't handle the FLUSH if not yet joined to the group
	    return digest;

	if(flush_dests == null) {
	     if(warn) log.warn("flush dest is null, ignoring flush !");
	    return digest;
	}

	if(flush_dests.size() == 0) {
	     if(warn) log.warn("flush dest is empty, ignoring flush !");
	    return digest;
	}

	if(!flush_dests.contains(local_addr)) {

		if(warn) log.warn("am not in the flush dests, ignoring flush");
	    return digest;
	}

	// block sending of messages (only if not already blocked !)
	if(!blocked) {
	    blocked=true;
	    synchronized(block_mutex) {
		passUp(new Event(Event.BLOCK));
		try {block_mutex.wait(block_timeout);}
		catch(InterruptedException e) {
		  Thread.currentThread().interrupt(); // GemStoneAddition
                  // Just keep going, we've propagated the bit.
                }
	    }
	}

	// asks NAKACK layer for unstable messages and saves result in 'digest'
	getMessageDigest(highest_seqnos);
	 if(log.isInfoEnabled()) log.info(ExternalStrings.FLUSH_RETURNING_DIGEST___0, digest);
	return digest;
    }






    /** Returns the highest seqnos (for each member) seen so far (using the NAKACK layer) */
    void getHighestDeliveredSeqnos() {
	synchronized(highest_delivered_mutex) {
	    passDown(new Event(Event.GET_MSGS_RECEIVED));
	    try {
		highest_delivered_mutex.wait(4000);
	    }
	    catch(InterruptedException e) {
              Thread.currentThread().interrupt(); // GemStoneAddition
              // Just propagate to caller
//		if(log.isDebugEnabled()) log.debug("exception is " + e);
	    }
	}
    }





    /** Interacts with a lower layer to retrieve unstable messages (e.g. NAKACK) */
    void getMessageDigest(long[] highest_seqnos) {
	synchronized(digest_mutex) {
	    passDown(new Event(Event.GET_MSG_DIGEST, highest_seqnos));
	    try {
		digest_mutex.wait(digest_timeout);
	    }
	    catch(InterruptedException e) {
	      Thread.currentThread().interrupt(); // GemStoneAddition
              // just propagate to caller
            }
	}
    }







    /**
       <b>Callback</b>. Called by superclass when event may be handled.<p>
       <b>Do not use <code>PassUp</code> in this method as the event is passed up
       by default by the superclass after this method returns !</b>
       @return boolean Defaults to true. If false, event will not be passed up the stack.
     */
    @Override // GemStoneAddition
    public boolean handleUpEvent(Event evt) {
	switch(evt.getType()) {

	case Event.SET_LOCAL_ADDRESS:
	    local_addr=(Address)evt.getArg();
	    break;

	case Event.GET_MSG_DIGEST_OK:
	    synchronized(digest_mutex) {
		digest=(Digest)evt.getArg();
		digest_mutex.notifyAll();
	    }
	    return false;  // don't pass further up

	case Event.GET_MSGS_RECEIVED_OK:
	    long[] tmp=(long[])evt.getArg();
	    if(tmp != null)
            System.arraycopy(tmp, 0, highest_delivered_msgs, 0, tmp.length);
	    synchronized(highest_delivered_mutex) {
		highest_delivered_mutex.notifyAll();
	    }
	    return false; // don't pass up any further !

	case Event.GET_MSGS_OK:
	    synchronized(get_msgs_mutex) {
		get_msgs=(List)evt.getArg();
		get_msgs_mutex.notifyAll();
	    }
	    break;

	}
	return true;
    }


    /**
       <b>Callback</b>. Called by superclass when event may be handled.<p>
       <b>Do not use <code>PassDown</code> in this method as the event is passed down
       by default by the superclass after this method returns !</b>
       @return boolean Defaults to true. If false, event will not be passed down the stack.
    */
    @Override // GemStoneAddition
    public boolean handleDownEvent(Event evt) {
	Vector    dests;
	FlushRsp  rsp;

	switch(evt.getType()) {
	case Event.FLUSH:
	    dests=(Vector)evt.getArg();
	    if(dests == null) dests=new Vector();
	    rsp=flush(dests);
	    passUp(new Event(Event.FLUSH_OK, rsp));
	    return false; // don't pass down

	case Event.BECOME_SERVER:
	    is_server=true;
	    break;

	case Event.VIEW_CHANGE:
	    blocked=false;

	    Vector tmp=((View)evt.getArg()).getMembers();
	    if(tmp != null) {
		mbrs.removeAllElements();
		for(int i=0; i < tmp.size(); i++)
		    mbrs.addElement(tmp.elementAt(i));
	    }
	    break;
	}
	return true;
    }





    /**
       The default handling adds the event to the down-queue where events are handled in order of
       addition by a thread. However, there exists a deadlock between the FLUSH and BLOCK_OK down
       events: when a FLUSH event is received, a BLOCK is sent up, which triggers a BLOCK_OK event
       to be sent down to be handled by the FLUSH layer. However, the FLUSH layer's thread is still
       processing the FLUSH down event and is therefore blocked, waiting for a BLOCK_OK event.
       Therefore, the BLOCK_OK event has to 'preempt' the FLUSH event processing. This is done by
       overriding this method: when a BLOCK_OK event is received, it is processed immediately
       (in parallel to the FLUSH event), which causes the FLUSH event processing to return.
    */
    @Override // GemStoneAddition
    public void receiveDownEvent(Event evt) {
	if(evt.getType() == Event.BLOCK_OK) { // priority handling, otherwise FLUSH would block !
	    synchronized(down_queue) {
		Event event;
		try {
		    while(down_queue.size() > 0) {
			event=(Event)down_queue.remove(10); // wait 10ms at most; queue is *not* empty !
			down(event);
		    }
		}
		catch(Exception e) {}
	    }

	    synchronized(block_mutex) {
		block_mutex.notifyAll();
	    }
	    return;
	}
	super.receiveDownEvent(evt);
    }



    @Override // GemStoneAddition
    public boolean setProperties(Properties props) {super.setProperties(props);
	String     str;

	str=props.getProperty("block_timeout");
	if(str != null) {
	    block_timeout=Long.parseLong(str);
	    props.remove("block_timeout");
	}

	str=props.getProperty("digest_timeout");
	if(str != null) {
	    digest_timeout=Long.parseLong(str);
	    props.remove("digest_timeout");
	}

	if(props.size() > 0) {
	    log.error(ExternalStrings.FLUSH_EXAMPLESETPROPERTIES_THESE_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);

	    return false;
	}
	return true;
    }



}

