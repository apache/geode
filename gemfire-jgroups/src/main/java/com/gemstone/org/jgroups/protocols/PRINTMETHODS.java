/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: PRINTMETHODS.java,v 1.3 2004/03/30 06:47:21 belaban Exp $

package com.gemstone.org.jgroups.protocols;



import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.blocks.MethodCall;
import com.gemstone.org.jgroups.stack.Protocol;


public class PRINTMETHODS extends Protocol {

    public PRINTMETHODS() {}

    @Override // GemStoneAddition  
    public String        getName()             {return "PRINTMETHODS";}


    @Override // GemStoneAddition  
    public void up(Event evt) {
	Object       obj=null;
//	byte[]       buf; GemStoneAddition
    	Message      msg;

	if(evt.getType() == Event.MSG) {
	    msg=(Message)evt.getArg();
	    if(msg.getLength() > 0) {
		try {
		    obj=msg.getObject();
		    if(obj != null && obj instanceof MethodCall)
			System.out.println("--> PRINTMETHODS: received " + obj);
		}
		catch(ClassCastException cast_ex) {}
		catch(Exception e) {}
	    }
	}

	passUp(evt);
    }
    


    @Override // GemStoneAddition  
    public void down(Event evt) {
//	Object       obj=null; GemStoneAddition
//	byte[]       buf; GemStoneAddition
//	Message      msg; GemStoneAddition

	if(evt.getType() == Event.MSG) {

	}
	passDown(evt);
    }




}
