/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: DUMMY.java,v 1.4 2005/08/08 12:45:42 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.Event;
//import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.stack.Protocol;

import java.util.Vector;


/**

 */

public class DUMMY extends Protocol {
    final Vector   members=new Vector();
    static/*GemStoneAddition*/ final String   name="DUMMY";

    /** All protocol names have to be unique ! */
    @Override // GemStoneAddition
    public String  getName() {return "DUMMY";}



    /** Just remove if you don't need to reset any state */
    public static void reset() {}




    @Override // GemStoneAddition
    public void up(Event evt) {
//	Message msg; GemStoneAddition

	switch(evt.getType()) {

	case Event.MSG:
//	    msg=(Message)evt.getArg() GemStoneAddition;
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
	    break;
	}

	passDown(evt);          // Pass on to the layer below us
    }



}
