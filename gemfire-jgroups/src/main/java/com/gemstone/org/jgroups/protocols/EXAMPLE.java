/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: EXAMPLE.java,v 1.4 2005/08/08 12:45:42 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.Event;
//import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.stack.Protocol;

import java.io.Serializable;
import java.util.Vector;



class ExampleHeader implements Serializable {
    private static final long serialVersionUID = -8802317525466899597L;
    // your variables

    ExampleHeader() {
    }

    @Override // GemStoneAddition
    public String toString() {
	return "[EXAMPLE: <variables> ]";
    }
}


/**
 * Example of a protocol layer. Contains no real functionality, can be used as a template.
 */

public class EXAMPLE extends Protocol {
    final Vector   members=new Vector();

    /** All protocol names have to be unique ! */
    @Override // GemStoneAddition
    public String  getName() {return "EXAMPLE";}





    /** Just remove if you don't need to reset any state */
    public static void reset() {}




    @Override // GemStoneAddition
    public void up(Event evt) {
//	Message msg; GemStoneAddition

	switch(evt.getType()) {

	case Event.MSG:
//	    msg=(Message)evt.getArg(); GemStoneAddition
	    // Do something with the event, e.g. extract the message and remove a header.
	    // Optionally pass up
	    break;
	}

	passUp(evt);            // Pass up to the layer above us
    }





    @Override // GemStoneAddition
    public void down(Event evt) {
//	Message msg; GemStoneAddition

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
//	    msg=(Message)evt.getArg(); GemStoneAddition
	    // Do something with the event, e.g. add a header to the message
	    // Optionally pass down
	    break;
	}

	passDown(evt);          // Pass on to the layer below us
    }



}
