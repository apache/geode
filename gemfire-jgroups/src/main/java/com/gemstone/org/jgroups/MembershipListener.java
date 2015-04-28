/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: MembershipListener.java,v 1.4 2005/07/17 11:38:05 chrislott Exp $

package com.gemstone.org.jgroups;



/**
 * Allows a listener to be notified when group membership changes.
 * These callbacks are used in {@link com.gemstone.org.jgroups.blocks.PullPushAdapter}.
 * <p>
 * The MembershipListener interface is similar to the {@link MessageListener} 
 * interface: every time a new view, a suspicion message, or a 
 * block event is received, the corresponding method of the class implementing  
 * MembershipListener will be called.
 * Oftentimes the only method containing any functionality will be viewAccepted() 
 * which notifies the receiver that a new member has joined the group or that an 
 * existing member has left or crashed.  
 */ 
public interface MembershipListener {
    
    /**
     * Called when a change in membership has occurred.
     * <b>No long running actions should be done in this callback.</b>
     * If some long running action needs to be performed, it should be done in a separate thread.
     */
    void viewAccepted(View new_view);
    
    /** 
     * Called whenever a member is suspected of having crashed, 
     * but has not yet been excluded. 
     */
    void suspect(SuspectMember suspected); // GemStoneAddition

    /** 
     * Called whenever the member needs to stop sending messages. 
     * When the next view is received (viewAccepted()), the member can resume sending 
     * messages. If a member does not comply, the message(s) sent between a block() 
     * and a matching viewAccepted() callback will probably be delivered in the next view.
     * The block() callback is only needed by the Virtual Synchrony suite of protocols 
     * (FLUSH protocol)3.2, otherwise it will never be invoked. 
     */
    void block();
    
    /**
     * GemStoneAddition - Called when the channel is closing
     */
    void channelClosing(Channel channel, Exception e);

}
