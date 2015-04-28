/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: LOSS.java,v 1.3 2004/09/23 16:29:41 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.Util;

import java.util.Vector;


/**
 * Example of a protocol layer. Contains no real functionality, can be used as a template.
 */

public class LOSS extends Protocol {
    final Vector    members=new Vector();
    static/*GemStoneAddition*/ final long      i=0;
    boolean   drop_next_msg=false;

    /** All protocol names have to be unique ! */
    @Override // GemStoneAddition  
    public String  getName() {return "LOSS";}



    /** Just remove if you don't need to reset any state */
    public void reset() {}




//      public void up(Event evt) {
//  	Message msg;

//  	switch(evt.getType()) {

//  	case Event.MSG:
//  	    msg=(Message)evt.getArg();
//  	    if(msg.getDest() != null && !((Address)msg.getDest()).isMulticastAddress()) {
//  		// System.err.println("LOSS.up(): not dropping msg as it is unicast !");
//  		break;
//  	    }

//  	    i++;

//  	    int r=((int)(Math.random() * 1000)) % 10;

//  	    if(r != 0 && i % r == 0) { // drop
//  		System.out.println("####### LOSS.up(): dropping message " + 
//  				   Util.printEvent(evt));
//  		return;
//  	    }

//  	    break;
//  	}

//  	passUp(evt);            // Pass up to the layer above us
//      }





    @Override // GemStoneAddition  
    public void down(Event evt) {
	Message msg;
	
	switch(evt.getType()) {

	case Event.TMP_VIEW:
	case Event.VIEW_CHANGE:
	    Vector new_members=((View)evt.getArg()).getMembers();
	    synchronized(members) {
		members.removeAllElements();
		if(new_members != null && new_members.size() > 0)
		    for(int i=0; i < new_members.size(); i++)
			members.addElement(new_members.elementAt(i));
	    }
	    passDown(evt);
	    break;

	case Event.MSG:
	    if(drop_next_msg) {
		drop_next_msg=false;
		msg=(Message)evt.getArg();

		if(msg.getDest() != null && !msg.getDest().isMulticastAddress()) {
		    break;
		}


		    System.out.println("###### LOSS.down(): dropping msg " + Util.printMessage(msg));
		
		return;
	    }
	    break;

	case Event.DROP_NEXT_MSG:
	    drop_next_msg=true;
	    break;
	}



	passDown(evt);          // Pass on to the layer below us
    }



}
