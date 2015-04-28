/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.stack.Protocol;

import java.util.Vector;




/**
 * Title: Flow control layer
 * Description: This layer limits the number of sent messages without a receive of an own message to MAXSENTMSGS,
 * just put this layer above GMS and you will get a more
 * Copyright:    Copyright (c) 2000
 * Company:      Computer Network Laboratory
 * @author Gianluca Collot
 * @version 1.0
 */
public class FLOWCONTROL extends Protocol {

  final Vector queuedMsgs = new Vector();
  int sentMsgs = 0;
  static final int MAXSENTMSGS = 1;
  Address myAddr;


    public FLOWCONTROL() {
    }
    @Override // GemStoneAddition  
    public String getName() {
	return "FLOWCONTROL";
    }

  /**
   * Checs if up messages are from myaddr and in the case sends down queued messages or
   * decremnts sentMsgs if there are no queued messages
   */
    @Override // GemStoneAddition  
    public void up(Event evt) {
	Message msg;
	switch (evt.getType()) {
	case Event.SET_LOCAL_ADDRESS: myAddr = (Address) evt.getArg();
	    break;

	case Event.MSG:               msg = (Message) evt.getArg();
	     if(log.isDebugEnabled()) log.debug("Message received");
	    if (msg.getSrc().equals(myAddr)) {
		if (queuedMsgs.size() > 0) {
		     if(log.isDebugEnabled()) log.debug("Message from me received - Queue size was " + queuedMsgs.size());
		    passDown((Event) queuedMsgs.remove(0));
		} else {
		     if(log.isDebugEnabled()) log.debug("Message from me received - No messages in queue");
		    sentMsgs--;
		}
	    }
	}
	passUp(evt);
    }

  /**
   * Checs if it can send the message, else puts the message in the queue.
   */
    @Override // GemStoneAddition  
    public void down(Event evt) {
	Message msg;
	if (evt.getType()==Event.MSG) {
	    msg = (Message) evt.getArg();
	    if ((msg.getDest() == null) || (msg.getDest().equals(myAddr))) {
		if (sentMsgs < MAXSENTMSGS) {
		    sentMsgs++;
		     if(log.isDebugEnabled()) log.debug("Message " + sentMsgs + " sent");
		} else {
		    queuedMsgs.add(evt); //queues message (we add the event to avoid creating a new event to send the message)
		    return;
		}
	    }
	}
	passDown(evt);
    }

}
