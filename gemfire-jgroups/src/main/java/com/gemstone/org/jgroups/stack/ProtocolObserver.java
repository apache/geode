/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ProtocolObserver.java,v 1.1.1.1 2003/09/09 01:24:12 belaban Exp $

package com.gemstone.org.jgroups.stack;


import com.gemstone.org.jgroups.Event;


/**
 * Interface for the Debugger to receive notifications about a protocol layer. Defines the
 * hooks called by Protocol when significant events occur, e.g. an event has been received.
 * Every ProtocolObserver should have a reference to the protocol it monitors.
 * @author  Bela Ban, July 22 2000
 */
public interface ProtocolObserver {

    /**
       Called when a ProtocolObserver is attached to a protcol. This reference can be used to
       modify the up-/down-queues, reorder events, inject new events etc.
     */
    void setProtocol(Protocol prot);
    

    
    /** Called when an event is about to be dispatched to the protocol (before it is dispatched).
	The up handler thread will block until this method returns. This allows an implementor
	to block indefinitely, and only process single events at a time, e.g. for single-stepping.
	For example, upon clicking on a button "Step" in the Debugger GUI, the method would unblock
	(waiting on a mutex, GUI thread notifies mutex).
	@param evt The event to be processed by the protocol. <em>This is not a copy, so changes
	to the event will be seen by the protocol !</em>
	@param num_evts The number of events currently in the up-queue (including this event).
	This number may increase while we're in the callback as the up-handler thread in the
	upper protocol layer puts new events into the up queue.
	@return boolean If true the event is processed, else it will be discarded (not be given
	to the protocol layer to process).
    */
    boolean up(Event evt, int num_evts);


    
    /** Called when an event is about to be passed up to the next higher protocol.	
	@param evt The event to be processed by the protocol. <em>This is not a copy, so changes
	to the event will be seen by the protocol !</em>
	@return boolean If true the event is passed up, else it will be discarded (not be given
	to the protocol layer above to process).
    */
    boolean passUp(Event evt);



    
    /** Called when an event is about to be dispatched to the protocol (before it is dispatched).
	The down handler thread will block until this method returns. This allows an implementor
	to block indefinitely, and only process single events at a time, e.g. for single-stepping.
	For example, upon clicking on a button "Step" in the Debugger GUI, the method would unblock
	(waiting on a mutex, GUI thread notifies mutex).
	@param evt The event to be processed by the protocol. <em>This is not a copy, so changes
	to the event will be seen by the protocol !</em>
	@param num_evts The number of events currently in the down-queue (including this event).
	This number may increase while we're in the callback as the down-handler thread in the
	upper protocol layer puts new events into the down queue.
	@return boolean If true the event is processed, else it will be discarded (not be given
	to the protocol layer to process).
    */
    boolean down(Event evt, int num_evts); 


    
    /** Called when an event is about to be passed down to the next lower protocol.	
	@param evt The event to be processed by the protocol. <em>This is not a copy, so changes
	to the event will be seen by the protocol !</em>
	@return boolean If true the event is passed down, else it will be discarded (not be given
	to the protocol layer below to process).
    */
    boolean passDown(Event evt);
}
