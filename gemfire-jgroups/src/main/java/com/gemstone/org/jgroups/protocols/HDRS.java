/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: HDRS.java,v 1.2 2004/03/30 06:47:21 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.stack.Protocol;


/**
 * Example of a protocol layer. Contains no real functionality, can be used as a template.
 */
public class HDRS extends Protocol {
  @Override // GemStoneAddition  
    public String  getName() {return "HDRS";}


    private void printMessage(Message msg, String label) {
	System.out.println("------------------------- " + label + " ----------------------");
	System.out.println(msg);
	msg.printObjectHeaders();
	System.out.println("--------------------------------------------------------------");
    }


    @Override // GemStoneAddition  
    public void up(Event evt) {
 	if(evt.getType() == Event.MSG) {
 	    Message msg=(Message)evt.getArg();
 	    printMessage(msg, "up");
 	}
	passUp(evt); // Pass up to the layer above us
    }



    @Override // GemStoneAddition  
    public void down(Event evt) {
 	if(evt.getType() == Event.MSG) {
 	    Message msg=(Message)evt.getArg();
 	    printMessage(msg, "down");
	}

	passDown(evt);  // Pass on to the layer below us
    }


}
